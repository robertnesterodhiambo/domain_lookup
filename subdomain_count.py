import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

INPUT_FILE = 'data_rdap_parsed.csv'
OUTPUT_FILE = 'domain_count.csv'
MAX_WORKERS = 20

lock = Lock()
outfile_lock = Lock()
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

# Step 1: Load already processed domains
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_domains.add(row['domain'].strip())

# Step 2: Read and prioritize input rows (.nl first)
with open(INPUT_FILE, newline='', encoding='utf-8') as infile:
    all_rows = list(csv.DictReader(infile))
    fieldnames = all_rows[0].keys() if all_rows else []
    fieldnames = list(fieldnames) + ['subdomain_count']

    # Sort: .nl domains first
    all_rows.sort(key=lambda row: (not row.get('domain', '').strip().endswith('.nl'), row.get('domain', '')))

# Prepare output file
write_header = not os.path.exists(OUTPUT_FILE)

def process_row(row):
    global write_header

    domain = row.get('domain', '').strip()
    if not domain or domain in processed_domains:
        return None

    sub_count = count_subdomains(domain)
    row['subdomain_count'] = sub_count

    with outfile_lock:
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
                f.flush()
                write_header = False
            writer.writerow(row)
            f.flush()
        print(f"✅ {domain}: {sub_count} subdomains written.")

    with lock:
        processed_domains.add(domain)

# Step 3: Process with threads
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_row, row) for row in all_rows]
    for future in as_completed(futures):
        _ = future.result()
