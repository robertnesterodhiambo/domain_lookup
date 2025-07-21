import subprocess
import time
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

LOOKUP_FILE = 'looku_file/lookup.txt'
OUTPUT_FILE = 'whois_results.csv'
RETRY_DELAY = 6
MAX_RETRIES = 1
BATCH_SIZE = 30
WHOIS_BIN = os.path.expanduser("~/go/bin/whois")

# Ensure output CSV exists
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "domain_name", "registrar", "creation_date", "expiration_date",
            "updated_date", "status", "name_servers", "raw_whois", "lookup_time"
        ])

# Read all domains
with open(LOOKUP_FILE, 'r', encoding='utf-8', errors='ignore') as f:
    all_domains = [line.strip() for line in f if line.strip()]

# Skip already processed domains
processed = set()
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        next(f)  # skip header
        for row in csv.reader(f):
            processed.add(row[0])

# Filter unprocessed domains
to_process = [d for d in all_domains if d not in processed]

print(f"Total: {len(all_domains)} | Already done: {len(processed)} | Remaining: {len(to_process)}")

def run_whois(domain):
    for _ in range(MAX_RETRIES + 1):
        try:
            result = subprocess.run(
                [WHOIS_BIN, "-d", domain],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=15,
                text=True
            )
            output = result.stdout.strip()

            now = datetime.utcnow().isoformat()

            if output:
                return {
                    "domain_name": domain,
                    "raw_whois": output,
                    "lookup_time": now
                }
            else:
                return {
                    "domain_name": domain,
                    "raw_whois": "NO DATA",
                    "lookup_time": now
                }
        except Exception as e:
            time.sleep(RETRY_DELAY)
    return {
        "domain_name": domain,
        "raw_whois": "ERROR",
        "lookup_time": datetime.utcnow().isoformat()
    }

def write_result(row):
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            row["domain_name"],
            "", "", "", "", "", "",  # empty WHOIS fields (not parsed here)
            row["raw_whois"],
            row["lookup_time"]
        ])

# Run multithreaded WHOIS
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(run_whois, domain): domain for domain in to_process[:10000]}  # first 10k

    for future in as_completed(futures):
        result = future.result()
        print(f"Processed: {result['domain_name']} | Status: {result['raw_whois'][:40]}...")
        write_result(result)
