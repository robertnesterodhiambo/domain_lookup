import whois
import time
import csv
import os

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
BATCH_SIZE = 100
RETRY_DELAY = 15  # seconds

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
            'raw_text'
        ])

def run_whois_with_retry(domain):
    attempts = 2  # One initial attempt + one retry
    for i in range(attempts):
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
            if "timed out" in err_text or "Connection reset" in err_text:
                if i < attempts - 1:
                    print(f"Retrying after {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                    continue
            return None, err_text
    return None, "FAILED_AFTER_RETRY"

def process_batch(domains):
    for domain in domains:
        domain = domain.strip()
        if not domain:
            continue
        result, error = run_whois_with_retry(domain)
        row = {
            'domain': domain,
            'domain_name': result.domain_name if result else None,
            'registrar': result.registrar if result else None,
            'creation_date': result.creation_date if result else None,
            'expiration_date': result.expiration_date if result else None,
            'updated_date': result.updated_date if result else None,
            'status': result.status if result else None,
            'name_servers': ','.join(result.name_servers) if result and result.name_servers else None,
            'emails': ','.join(result.emails) if result and result.emails else None,
            'raw_text': error if error else None
        }

        # Write immediately
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writerow(row)

        # Delay after each request to reduce risk of blocking
        time.sleep(10)

def process_lookup_file():
    with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        batch = []
        for line in f:
            batch.append(line)
            if len(batch) >= BATCH_SIZE:
                process_batch(batch)
                batch = []
        # Process any remaining domains
        if batch:
            process_batch(batch)

if __name__ == '__main__':
    process_lookup_file()
