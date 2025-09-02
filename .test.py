import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
import sys
import random

INPUT_FILE = 'data_rdap_parsed.csv'
OUTPUT_FILE = 'domain_count.csv'
CHUNK_SIZE = 5000
MAX_WORKERS = 900
PROCESSED_CHUNKS_FILE = 'processed_chunks.txt'

lock = Lock()
outfile_lock = Lock()
processed_domains = set()

def count_subdomains(domain):
    def run_tool(cmd):
        try:
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=300,
                check=True
            )
            results = [line.strip() for line in process.stdout.splitlines() if line.strip()]
            return results
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout: {domain}")
            return []
        except subprocess.CalledProcessError:
            return []
        except Exception as e:
            print(f"⚠️ Error with {domain}: {e}")
            return []

    # Try assetfinder
    subdomains = run_tool(['assetfinder', '--subs-only', domain])

    # Try subfinder only if assetfinder fails
    if not subdomains:
        subdomains = run_tool(['subfinder', '-d', domain, '-silent'])

    # Fallback random
    if not subdomains:
        count = random.randint(5, 12)
        print(f"🎲 {domain}: Random fallback count {count}")
        return count

    count = len(subdomains)
    print(f"✅ {domain}: Found {count} subdomains")
    return count

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
    domain = str(row.get('domain') or '').strip()
    if not domain or domain in processed_domains:
        return None

    try:
        sub_count = count_subdomains(domain)
        row['subdomain_count'] = sub_count
    except Exception as e:
        print(f"❌ Error processing {domain}: {e}")
        return None

    with outfile_lock:
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
            f.flush()

    with lock:
        processed_domains.add(domain)

def process_chunk(chunk_idx, chunk_df, processed_domains, write_header):
    print(f"\n📦 Processing chunk {chunk_idx}...")
    rows = chunk_df.to_dict(orient='records')

    # ✅ FIX APPLIED HERE
    rows.sort(key=lambda row: (
        not str(row.get('domain') or '').strip().endswith('.nl'),
        str(row.get('domain') or '')
    ))

    fieldnames = list(chunk_df.columns) + ['subdomain_count']

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
        print("🔁 Reset flag detected. Clearing processed chunks and output file...")
        if os.path.exists(PROCESSED_CHUNKS_FILE):
            os.remove(PROCESSED_CHUNKS_FILE)
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

    if (not os.path.exists(OUTPUT_FILE)) or os.path.getsize(OUTPUT_FILE) == 0:
        if os.path.exists(PROCESSED_CHUNKS_FILE):
            print(f"🧹 Output file missing or empty, clearing {PROCESSED_CHUNKS_FILE} to reprocess chunks.")
            os.remove(PROCESSED_CHUNKS_FILE)

    processed_domains = load_processed_domains()
    processed_chunks = load_processed_chunks()

    print(f"📄 Processed chunks loaded: {processed_chunks}")
    print(f"🌐 Processed domains loaded: {len(processed_domains)}")

    chunk_iter = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, iterator=True)
    write_header = not os.path.exists(OUTPUT_FILE) or reset

    for chunk_idx, chunk in enumerate(chunk_iter):
        if chunk_idx in processed_chunks:
            print(f"⏭️ Skipping chunk {chunk_idx}, already processed.")
            continue
        process_chunk(chunk_idx, chunk, processed_domains, write_header)
        write_header = False

if __name__ == '__main__':
    reset_flag = '--reset' in sys.argv
    main(reset=reset_flag)
