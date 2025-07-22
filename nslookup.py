import subprocess
import pandas as pd
import os
import threading
from concurrent.futures import ThreadPoolExecutor

INPUT_FILE = 'db_excel.csv'
OUTPUT_FILE = 'db_excel_ns.csv'
THREADS = 9999  # Adjust based on system capacity
LOCK = threading.Lock()

# DNS types to query
NS_TYPES = ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'PTR', 'TXT', 'SRV', 'SOA']

def get_processed_domains():
    """Return a set of already processed domain names from OUTPUT_FILE."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['domain'])
        return set(df['domain'].astype(str).str.strip())
    except Exception:
        return set()

def run_nslookups(domain):
    """Run nslookup for all DNS types and return results in a dict."""
    outputs = {}
    for qtype in NS_TYPES:
        try:
            result = subprocess.run(
                ['nslookup', '-q=' + qtype, domain],
                capture_output=True,
                text=True,
                timeout=10
            )
            outputs[f'nslookup{qtype}'] = result.stdout.strip()
        except Exception as e:
            outputs[f'nslookup{qtype}'] = f"Error: {e}"
    return outputs

def save_row(row_dict, columns):
    """Append a single row to the output CSV in a thread-safe way."""
    with LOCK:
        file_exists = os.path.exists(OUTPUT_FILE)
        df = pd.DataFrame([row_dict], columns=columns)
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=not file_exists)

def process_row(row, all_columns):
    """Process a single row: run nslookups, add results, and save."""
    domain = str(row['domain']).strip()
    ns_outputs = run_nslookups(domain)

    print(f"\n🔍 Domain: {domain}")
    for k, v in ns_outputs.items():
        print(f"  ├─ {k}: {len(v.splitlines())} lines")

    result = dict(row)
    result.update(ns_outputs)

    save_row(result, all_columns)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    if 'domain' not in df.columns:
        print("❌ 'domain' column is missing in input.")
        return

    processed = get_processed_domains()
    remaining = df[~df['domain'].astype(str).isin(processed)]

    print(f"✅ Already processed: {len(processed)}")
    print(f"🚀 Domains to process: {len(remaining)}")

    base_cols = df.columns.tolist()
    extra_cols = [f'nslookup{q}' for q in NS_TYPES]
    all_columns = base_cols + extra_cols

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for _, row in remaining.iterrows():
            executor.submit(process_row, row, all_columns)

if __name__ == '__main__':
    main()
