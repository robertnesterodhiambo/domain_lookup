import whois
import time
import csv
import os
import tldextract

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
RETRY_DELAY = 60  # seconds
BATCH_SIZE = 100

# Ensure the output file exists with headers
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'domain',
            'domain_name',
            'registrar',
            'creation_date',
            'expiration_date',
            'updated_date',
            'status',
            'name_servers',
            'emails',
            'country',
            'city',
            'tld',
            'raw_text'
        ])

# Load already collected domains
def load_collected_domains():
    collected = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                collected.add(row['domain'].strip().lower())
    return collected

def format_emails(emails):
    if not emails:
        return None
    if isinstance(emails, str):
        return emails.strip()
    elif isinstance(emails, list):
        return ','.join([e for e in emails if '@' in e and '.' in e])
    return None

def run_whois_with_retry(domain):
    while True:
        try:
            print(f"Processing domain: {domain}")
            data = whois.whois(domain)

            # Retry if response is empty or missing domain_name
            if not data or not getattr(data, 'domain_name', None):
                print(f"No valid WHOIS data for {domain} — possibly socket error. Waiting {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                continue

            return data, None

        except Exception as e:
            err_text = str(e)
            print(f"[{type(e).__name__}] Error for {domain}: {err_text}")
            print(f"Waiting {RETRY_DELAY} seconds before retrying {domain}...")
            time.sleep(RETRY_DELAY)

def process_batch(domains, collected_set):
    for domain in domains:
        domain = domain.strip().lower()
        if not domain:
            continue
        if domain in collected_set:
            print(f"Skipping already collected: {domain}")
            continue

        result, error = run_whois_with_retry(domain)

        ext = tldextract.extract(domain)
        tld = ext.suffix

        row = {
            'domain': domain,
            'domain_name': getattr(result, 'domain_name', None),
            'registrar': getattr(result, 'registrar', None),
            'creation_date': getattr(result, 'creation_date', None),
            'expiration_date': getattr(result, 'expiration_date', None),
            'updated_date': getattr(result, 'updated_date', None),
            'status': getattr(result, 'status', None),
            'name_servers': ','.join(result.name_servers) if result and result.name_servers else None,
            'emails': format_emails(getattr(result, 'emails', None)),
            'country': getattr(result, 'country', None),
            'city': getattr(result, 'city', None),
            'tld': tld,
            'raw_text': error if error else "SUCCESS"
        }

        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writerow(row)

        collected_set.add(domain)
        time.sleep(10)

def process_lookup_file():
    collected_set = load_collected_domains()
    with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        batch = []
        for line in f:
            batch.append(line)
            if len(batch) >= BATCH_SIZE:
                process_batch(batch, collected_set)
                batch = []
        if batch:
            process_batch(batch, collected_set)

if __name__ == '__main__':
    process_lookup_file()
