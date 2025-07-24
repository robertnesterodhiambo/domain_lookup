import subprocess
import pandas as pd
import os
import threading
from concurrent.futures import ThreadPoolExecutor

INPUT_FILE = 'domain_count.csv'
OUTPUT_FILE = 'nslookup.csv'
THREADS = 99
LOCK = threading.Lock()
CHUNK_SIZE = 50000

NS_TYPES = ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'PTR', 'TXT', 'SRV', 'SOA']
counter = 0  # global progress tracker

def get_processed_domains():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['domain'])
        return set(df['domain'].astype(str).str.strip())
    except Exception:
        return set()

def run_nslookups(domain):
    result = {}
    for qtype in NS_TYPES:
        try:
            lookup = subprocess.run(
                ['nslookup', '-q=' + qtype, domain],
                capture_output=True,
                text=True,
                timeout=8
            )
            result[f'nslookup{qtype}'] = lookup.stdout.strip()
        except Exception as e:
            result[f'nslookup{qtype}'] = f"Error: {e}"
    return result

def save_result(row_data, all_columns):
    with LOCK:
        file_exists = os.path.exists(OUTPUT_FILE)
        df = pd.DataFrame([row_data], columns=all_columns)
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=not file_exists)

        global counter
        counter += 1
        print(f"{counter} saved and processed")

def process_row(row, all_columns):
    domain = str(row['domain']).strip()
    ns_data = run_nslookups(domain)
    full_data = dict(row)
    full_data.update(ns_data)
    save_result(full_data, all_columns)

def process_chunk(chunk, processed_domains, all_columns):
    chunk['domain'] = chunk['domain'].astype(str).str.strip()
    remaining = chunk[~chunk['domain'].isin(processed_domains)]

    if remaining.empty:
        print("All domains in this chunk are already processed. Skipping chunk.")
        return

    exact_nl = remaining[remaining['domain'].str.match(r'^[^.]+\.(nl)$', na=False)]
    sub_nl = remaining[remaining['domain'].str.match(r'^.+\.(.+\.)?nl$', na=False) & ~remaining['domain'].str.match(r'^[^.]+\.(nl)$', na=False)]
    others = remaining[~remaining.index.isin(exact_nl.index) & ~remaining.index.isin(sub_nl.index)]
    prioritized = pd.concat([exact_nl, sub_nl, others], ignore_index=True)

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for _, row in prioritized.iterrows():
            executor.submit(process_row, row, all_columns)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    processed_domains = get_processed_domains()
    print(f"🟡 Already processed: {len(processed_domains)}")

    first_chunk = True
    for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE):
        print(f"🔹 Processing new chunk of size: {len(chunk)}")
        base_columns = chunk.columns.tolist()
        ns_columns = [f'nslookup{q}' for q in NS_TYPES]
        all_columns = base_columns + ns_columns

        process_chunk(chunk, processed_domains, all_columns)

if __name__ == '__main__':
    main()
