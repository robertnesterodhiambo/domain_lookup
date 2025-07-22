import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

INPUT_FILE = 'whois_results.csv'
OUTPUT_FILE = 'db_excel.csv'
MAX_WORKERS = 99

write_lock = Lock()
processed_domains = set()

def count_subdomains(domain):
    try:
        result = subprocess.run(
            ['subfinder', '-d', domain, '-silent'],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=True
        )
        return len(result.stdout.strip().splitlines())
    except subprocess.CalledProcessError:
        return 0

def process_row(row, fieldnames):
    domain = row.get('domain', '').strip()
    if not domain:
        print("⚠️ Skipping empty domain.")
        return None

    if domain in processed_domains:
        print(f"⏩ Skipping already processed domain: {domain}")
        return None

    sub_count = count_subdomains(domain)
    row['subdomain_count'] = sub_count

    with write_lock:
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writerow(row)
            outfile.flush()
        processed_domains.add(domain)
        print(f"✅ {domain}: {sub_count} subdomains written.")
    return domain

def prioritize_domains(rows):
    nl_domains = []
    others = []

    for row in rows:
        domain = row.get('domain', '').strip().lower()
        if domain.endswith('.nl'):
            nl_domains.append(row)
        else:
            others.append(row)

    return nl_domains + others

def main():
    global processed_domains

    # Load already processed domains
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_domains.add(row['domain'].strip())

    # Read input file and get rows to process
    with open(INPUT_FILE, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['subdomain_count']
        all_rows = list(reader)

        # Write header if needed
        if not os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

        # Prioritize .nl domains first
        prioritized_rows = prioritize_domains(all_rows)

        # Process with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_row, row, fieldnames) for row in prioritized_rows]

            for future in as_completed(futures):
                _ = future.result()

if __name__ == "__main__":
    main()
