import os
import csv
import subprocess
import time
import re
import whois
from datetime import datetime
from dateutil.parser import parse as parse_date

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
CHUNK_SIZE = 20000
RETRY_DELAY = 60  # not used now
HEADERS = [
    'domain', 'tld', 'registrar', 'whois_server', 'creation_date', 'updated_date',
    'expiration_date', 'status', 'registrant_name', 'registrant_org',
    'registrant_country', 'admin_email', 'tech_email', 'name_servers',
    'dnssec', 'lookup_date', 'source'
]

# Regex patterns to extract WHOIS fields
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
            print(f"Rate limit exceeded for {domain}. Switching to Python whois.")
            return parse_whois_python(domain)
        elif "No match for" in output or "NOT FOUND" in output.upper():
            return {'domain': domain, 'tld': extract_tld(domain), 'lookup_date': datetime.utcnow().isoformat(), 'source': 'no data'}
        else:
            return parse_whois_output(output, domain)
    except Exception as e:
        return {'domain': domain, 'tld': extract_tld(domain), 'lookup_date': datetime.utcnow().isoformat(), 'source': f'terminal-error: {str(e)}'}

def save_result(record):
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)

def process_chunk(domains):
    for domain in domains:
        domain = domain.strip()
        if not domain: continue
        record = run_whois(domain)
        save_result(record)
        print(record)

def main():
    if not os.path.exists(LOOKUP_FILE):
        print(f"{LOOKUP_FILE} not found.")
        return

    with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        buffer = []
        for i, line in enumerate(f, start=1):
            buffer.append(line.strip())
            if i % CHUNK_SIZE == 0:
                print(f"\nProcessing {CHUNK_SIZE} domains (up to line {i})...")
                process_chunk(buffer)
                buffer.clear()

        if buffer:
            print(f"\nProcessing remaining {len(buffer)} domains...")
            process_chunk(buffer)

if __name__ == '__main__':
    main()
