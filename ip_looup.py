import whois
import time
import csv
import os
import tldextract

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
RETRY_DELAY = 6  # seconds
MAX_RETRIES = 1
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

# Format emails properly
def format_emails(emails):
    if not emails:
        return None
    if isinstance(emails, str):
        return emails.strip()
    elif isinstance(emails, list):
        return ','.join([e for e in emails if '@' in e and '.' in e])
    return None

# Normalize date fields that may return lists
def normalize_date(date_field):
    if isinstance(date_field, list):
        return date_field[0]
    return date_field

# Run WHOIS with max 3 retries
def run_whois_with_retry(domain):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Processing domain: {domain} (Attempt {attempt})")
            data = whois.whois(domain)

            if not data or not getattr(data, 'domain_name', None):
                print(f"No valid WHOIS data for {domain} — possibly empty or bad data.")
                raise ValueError("No valid WHOIS data")

            return data, None

        except Exception as e:
            print(f"[{type(e).__name__}] Error for {domain}: {str(e)}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                return None, "NO_DATA"

# Process a list of domains
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
            'domain_name': getattr(result, 'domain_name', None) if result else None,
            'registrar': getattr(result, 'registrar', None) if result else None,
            'creation_date': normalize_date(getattr(result, 'creation_date', None)) if result else None,
            'expiration_date': normalize_date(getattr(result, 'expiration_date', None)) if result else None,
            'updated_date': normalize_date(getattr(result, 'updated_date', None)) if result else None,
            'status': getattr(result, 'status', None) if result else None,
            'name_servers': ','.join(result.name_servers) if result and result.name_servers else None,
            'emails': format_emails(getattr(result, 'emails', None)) if result else None,
            'country': getattr(result, 'country', None) if result else None,
            'city': getattr(result, 'city', None) if result else None,
            'tld': tld,
            'raw_text': error if error else "SUCCESS"
        }

        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writerow(row)

        collected_set.add(domain)
        time.sleep(10)

# Read and process lookup file
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
