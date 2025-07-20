import subprocess
import time
import csv
import os
import re
from datetime import datetime

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
RETRY_DELAY = 6
MAX_RETRIES = 1
BATCH_SIZE = 100

# Create output file with correct headers
HEADERS = [
    'domain_name',
    'registry_domain_id',
    'registrar_url',
    'registrar',
    'registrar_abuse_email',
    'registrar_abuse_phone',
    'creation_date',
    'updated_date',
    'expiry_date',
    'registrant_country',
    'registrant_state',
    'registrant_org',
    'admin_country',
    'admin_state',
    'admin_org',
    'tech_country',
    'tech_state',
    'tech_org',
    'name_servers',
    'dnssec',
    'domain_status',
    'date_searched'
]

if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

def run_whois_terminal(domain):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n🔍 Processing domain: {domain} (Attempt {attempt})")
            result = subprocess.run(['whois', domain], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
            output = result.stdout.strip()
            print("----- WHOIS OUTPUT BEGIN -----")
            print(output)
            print("----- WHOIS OUTPUT END -------\n")

            if not output or 'No match' in output or 'NOT FOUND' in output.upper():
                raise ValueError("No valid WHOIS data")

            return output, None

        except Exception as e:
            print(f"[{type(e).__name__}] Error for {domain}: {str(e)}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                return None, "NO_DATA"

def extract_field(text, field_name):
    match = re.search(rf'^{re.escape(field_name)}:\s*(.+)', text, re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else None

def extract_all_fields(whois_text):
    return {
        'domain_name': extract_field(whois_text, 'Domain Name'),
        'registry_domain_id': extract_field(whois_text, 'Registry Domain ID'),
        'registrar_url': extract_field(whois_text, 'Registrar URL'),
        'registrar': extract_field(whois_text, 'Registrar'),
        'registrar_abuse_email': extract_field(whois_text, 'Registrar Abuse Contact Email'),
        'registrar_abuse_phone': extract_field(whois_text, 'Registrar Abuse Contact Phone'),
        'creation_date': extract_field(whois_text, 'Creation Date'),
        'updated_date': extract_field(whois_text, 'Updated Date'),
        'expiry_date': extract_field(whois_text, 'Registry Expiry Date'),
        'registrant_country': extract_field(whois_text, 'Registrant Country'),
        'registrant_state': extract_field(whois_text, 'Registrant State/Province'),
        'registrant_org': extract_field(whois_text, 'Registrant Organization'),
        'admin_country': extract_field(whois_text, 'Admin Country'),
        'admin_state': extract_field(whois_text, 'Admin State/Province'),
        'admin_org': extract_field(whois_text, 'Admin Organization'),
        'tech_country': extract_field(whois_text, 'Tech Country'),
        'tech_state': extract_field(whois_text, 'Tech State/Province'),
        'tech_org': extract_field(whois_text, 'Tech Organization'),
        'name_servers': ', '.join(re.findall(r'^Name Server:\s*(\S+)', whois_text, re.MULTILINE | re.IGNORECASE)),
        'dnssec': extract_field(whois_text, 'DNSSEC'),
        'domain_status': extract_field(whois_text, 'Domain Status'),
        'date_searched': datetime.utcnow().isoformat()
    }

def process_batch(domains):
    for domain in domains:
        domain = domain.strip().lower()
        if not domain:
            continue

        whois_text, error = run_whois_terminal(domain)
        if not whois_text:
            continue

        data = extract_all_fields(whois_text)
        print("✅ Parsed Data:")
        for k, v in data.items():
            print(f"  {k}: {v}")

        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writerow(data)

        time.sleep(10)

def process_lookup_file():
    with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        batch = []
        for line in f:
            batch.append(line)
            if len(batch) >= BATCH_SIZE:
                process_batch(batch)
                batch = []
        if batch:
            process_batch(batch)

if __name__ == '__main__':
    process_lookup_file()
