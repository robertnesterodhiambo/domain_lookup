import whois
import time
import csv
import os
import tldextract

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
RETRY_DELAY = 15  # seconds
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
            if data and data.domain_name:
                return data, None
            else:
                return None, "NO_DATA"
        except Exception as e:
            err_text = str(e)
            print(f"Error for {domain}: {err_text}")
            # Retry only if connection reset or similar
            if "Connection reset by peer" in err_text or "timed out" in err_text or True:
                print(f"Retrying after {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                continue
            return None, err_text

def process_batch(domains):
    for domain in domains:
        domain = domain.strip()
        if not domain:
            continue

        result, error = run_whois_with_retry(domain)

        # Extract TLD
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
            'raw_text': error if error else None
        }

        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writerow(row)

        time.sleep(10)  # delay to avoid blocking

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
