import os
import csv
import subprocess
import re
import whois
from datetime import datetime
from dateutil.parser import parse as parse_date
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
CHUNK_SIZE = 20000
MAX_WORKERS = 60
HEADERS = [
    'domain', 'tld', 'registrar', 'whois_server', 'creation_date', 'updated_date',
    'expiration_date', 'status', 'registrant_name', 'registrant_org',
    'registrant_country', 'admin_email', 'tech_email', 'name_servers',
    'dnssec', 'lookup_date', 'source'
]

REGEX_PATTERNS = {
    'registrar': r'Registrar:\s*(.+)',
    'whois_server': r'Registrar WHOIS Server:\s*(.+)',
    'creation_date': r'Creation Date:\s*(.+)',
    'updated_date': r'Updated Date:\s*(.+)',
    'expiration_date': r'Expiration Date:\s*(.+)',
    'name_servers': r'Name Server:\s*(.+)',
    'status': r'Domain Status:\s*(.+)',
    'registrant_name': r'Registrant Name:\s*(.+)',
    'registrant_org': r'Registrant Organization:\s*(.+)',
    'registrant_country': r'Registrant Country:\s*(.+)',
    'admin_email': r'Admin Email:\s*(.+)',
    'tech_email': r'Tech Email:\s*(.+)',
    'dnssec': r'DNSSEC:\s*(.+)',
}

lock = Lock()

def clean_date(value):
    try:
        if isinstance(value, list):
            return parse_date(str(value[0])).isoformat()
        return parse_date(str(value)).isoformat()
    except Exception:
        return ''

def extract_tld(domain):
    parts = domain.strip().split('.')
    return parts[-1] if len(parts) > 1 else ''

def parse_whois_output(raw_output, domain):
    data = {
        'domain': domain,
        'tld': extract_tld(domain),
        'lookup_date': datetime.utcnow().isoformat(),
        'source': 'terminal'
    }
    for key, pattern in REGEX_PATTERNS.items():
        matches = re.findall(pattern, raw_output, re.IGNORECASE)
        if matches:
            if key.endswith('date'):
                data[key] = clean_date(matches[0])
            elif key == 'name_servers':
                data[key] = ','.join(matches)
            elif key == 'status':
                data[key] = ';'.join(set(matches))
            else:
                data[key] = matches[0]
        else:
            data[key] = ''
    return data

def parse_whois_python(domain):
    try:
        w = whois.whois(domain)
        return {
            'domain': domain,
            'tld': extract_tld(domain),
            'registrar': w.registrar or '',
            'whois_server': w.whois_server or '',
            'creation_date': clean_date(w.creation_date),
            'updated_date': clean_date(w.updated_date),
            'expiration_date': clean_date(w.expiration_date),
            'status': ';'.join(w.status) if isinstance(w.status, list) else w.status or '',
            'registrant_name': w.get('name', ''),
            'registrant_org': w.get('org', ''),
            'registrant_country': w.get('country', ''),
            'admin_email': w.get('emails', ''),
            'tech_email': w.get('emails', ''),
            'name_servers': ','.join(w.name_servers) if isinstance(w.name_servers, list) else w.name_servers or '',
            'dnssec': w.dnssec or '',
            'lookup_date': datetime.utcnow().isoformat(),
            'source': 'python-whois'
        }
    except Exception as e:
        return {
            'domain': domain,
            'tld': extract_tld(domain),
            'lookup_date': datetime.utcnow().isoformat(),
            'source': f'python-whois-error: {str(e)}'
        }

def run_whois(domain):
    try:
        result = subprocess.run(['whois', domain], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
        output = result.stdout
        if "Number of allowed queries exceeded" in output or "rate limit" in output.lower():
            print(f"[RATE LIMIT] Switching to Python WHOIS for: {domain}")
            return parse_whois_python(domain)
        elif "No match for" in output or "NOT FOUND" in output.upper():
            return {'domain': domain, 'tld': extract_tld(domain), 'lookup_date': datetime.utcnow().isoformat(), 'source': 'no data'}
        else:
            return parse_whois_output(output, domain)
    except Exception as e:
        return {'domain': domain, 'tld': extract_tld(domain), 'lookup_date': datetime.utcnow().isoformat(), 'source': f'terminal-error: {str(e)}'}

def save_results_batch(records):
    with lock:
        file_exists = os.path.exists(OUTPUT_FILE)
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            if not file_exists or f.tell() == 0:
                writer.writeheader()
            writer.writerows(records)

def load_processed_domains():
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = row.get('domain', '').strip().lower()
                if domain:
                    processed.add(domain)
    return processed

def process_domain(domain, processed_domains):
    domain = domain.strip().lower()
    if not domain or domain in processed_domains:
        print(f"[SKIPPED] {domain}")
        return None
    record = run_whois(domain)
    processed_domains.add(domain)
    return record

def process_chunk(domains, processed_domains):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_domain, domain, processed_domains) for domain in domains]
        for future in as_completed(futures):
            result = future.result()
            if result:
                print(f"[OK] {result['domain']} -> {result['source']}")
                results.append(result)
    save_results_batch(results)

def main():
    if not os.path.exists(LOOKUP_FILE):
        print(f"[ERROR] {LOOKUP_FILE} not found.")
        return

    processed_domains = load_processed_domains()
    print(f"[INFO] Loaded {len(processed_domains)} already processed domains.")

    with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        domains = [line.strip() for line in f if line.strip()]

    # Prioritize .nl and .NL domains first
    nl_domains = [d for d in domains if d.lower().endswith('.nl')]
    other_domains = [d for d in domains if not d.lower().endswith('.nl')]
    sorted_domains = nl_domains + other_domains

    print(f"[INFO] Prioritized {len(nl_domains)} .nl domains")

    buffer = []
    for i, domain in enumerate(sorted_domains, start=1):
        buffer.append(domain)
        if i % CHUNK_SIZE == 0:
            print(f"\n[INFO] Processing {CHUNK_SIZE} domains (up to line {i})...")
            process_chunk(buffer, processed_domains)
            buffer.clear()

    if buffer:
        print(f"\n[INFO] Processing remaining {len(buffer)} domains...")
        process_chunk(buffer, processed_domains)

if __name__ == '__main__':
    main()
