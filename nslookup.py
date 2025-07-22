import subprocess
import pandas as pd
import re
import os
import threading
from concurrent.futures import ThreadPoolExecutor

INPUT_FILE = 'db_excel.csv'
OUTPUT_FILE = 'db_excel_ns.csv'
THREADS = 999  # You can increase this depending on your system
LOCK = threading.Lock()

def get_processed_domains():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['domain'])
        return set(df['domain'].astype(str).str.strip())
    except Exception:
        return set()

def run_nslookup(domain):
    try:
        result = subprocess.run(
            ['nslookup', domain],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout
    except Exception as e:
        return f"Error: {e}"

def extract_ips(nslookup_output):
    return re.findall(r'Address:\s+([0-9a-fA-F:.]+)', nslookup_output)

def save_row(row_dict, columns):
    with LOCK:
        file_exists = os.path.exists(OUTPUT_FILE)
        df = pd.DataFrame([row_dict], columns=columns)
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=not file_exists)

def process_row(row, all_columns):
    domain = str(row['domain']).strip()
    ns_output = run_nslookup(domain)
    ips = extract_ips(ns_output)

    # Print results to terminal
    print(f"\n🔍 Domain: {domain}")
    if ips:
        for i, ip in enumerate(ips):
            print(f"  ├─ IP {i+1}: {ip}")
    else:
        print("  └─ No IPs found.")

    result = dict(row)
    for i, ip in enumerate(ips):
        result[f'nslookup_ip_{i+1}'] = ip

    save_row(result, all_columns)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    if 'domain' not in df.columns:
        print("❌ 'domain' column is missing.")
        return

    processed = get_processed_domains()
    remaining = df[~df['domain'].astype(str).isin(processed)]

    print(f"✅ Already processed: {len(processed)}")
    print(f"🚀 Starting new: {len(remaining)} domains")

    base_cols = df.columns.tolist()
    extra_cols = [f'nslookup_ip_{i+1}' for i in range(10)]  # support up to 10 IPs
    all_columns = base_cols + extra_cols

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for _, row in remaining.iterrows():
            executor.submit(process_row, row, all_columns)

if __name__ == '__main__':
    main()

