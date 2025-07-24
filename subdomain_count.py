import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
import sys

INPUT_FILE = 'data_rdap_parsed.csv'
OUTPUT_FILE = 'domain_count.csv'
CHUNK_SIZE = 50000
MAX_WORKERS = 20
PROCESSED_CHUNKS_FILE = 'processed_chunks.txt'

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

def load_processed_domains():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return set(row['domain'].strip() for row in reader)
    return set()

def load_processed_chunks():
    if os.path.exists(PROCESSED_CHUNKS_FILE):
        with open(PROCESSED_CHUNKS_FILE, 'r', encoding='utf-8') as f:
            return set(int(line.strip()) for line in f if line.strip().isdigit())
    return set()

def save_processed_chunk(chunk_idx):
    with lock:
        with open(PROCESSED_CHUNKS_FILE, 'a', encoding='utf-8') as f:
            f.write(str(chunk_idx) + '\n')

def process_row(row, fieldnames, processed_domains):
    domain = row.get('domain', '').strip()
    if not domain or domain in processed_domains:
        return None

    sub_count = count_subdomains(domain)
    row['subdomain_count'] = sub_count

    with outfile_lock:
        # Append row to output file
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
            f.flush()
        print(f"✅ {domain}: {sub_count} subdomains written.")

    with lock:
        processed_domains.add(domain)

def process_chunk(chunk_idx, chunk_df, processed_domains, write_header):
    print(f"Processing chunk {chunk_idx}...")
    rows = chunk_df.to_dict(orient='records')
    # Sort .nl domains first
    rows.sort(key=lambda row: (not row.get('domain', '').strip().endswith('.nl'), row.get('domain', '')))
    fieldnames = list(chunk_df.columns) + ['subdomain_count']

    # Write header if needed
    if write_header:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_row, row, fieldnames, processed_domains) for row in rows]
        for future in as_completed(futures):
            _ = future.result()

    save_processed_chunk(chunk_idx)

def main(reset=False):
    global processed_domains

    if reset:
        print("Reset flag detected. Clearing processed chunks and output file...")
        if os.path.exists(PROCESSED_CHUNKS_FILE):
            os.remove(PROCESSED_CHUNKS_FILE)
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

    # If output file doesn't exist or is empty, clear processed_chunks.txt to avoid skipping
    if (not os.path.exists(OUTPUT_FILE)) or os.path.getsize(OUTPUT_FILE) == 0:
        if os.path.exists(PROCESSED_CHUNKS_FILE):
            print(f"Output file missing or empty, clearing {PROCESSED_CHUNKS_FILE} to reprocess chunks.")
            os.remove(PROCESSED_CHUNKS_FILE)

    processed_domains = load_processed_domains()
    processed_chunks = load_processed_chunks()

    print(f"Processed chunks loaded: {processed_chunks}")
    print(f"Processed domains loaded: {len(processed_domains)}")

    chunk_iter = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, iterator=True)

    write_header = not os.path.exists(OUTPUT_FILE) or reset

    for chunk_idx, chunk in enumerate(chunk_iter):
        if chunk_idx in processed_chunks:
            print(f"Skipping chunk {chunk_idx}, already processed.")
            continue
        process_chunk(chunk_idx, chunk, processed_domains, write_header)
        write_header = False  # header written after first chunk processed

if __name__ == '__main__':
    reset_flag = '--reset' in sys.argv
    main(reset=reset_flag)
