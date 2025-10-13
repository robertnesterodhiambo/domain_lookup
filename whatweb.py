import subprocess
import json
import pandas as pd
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

FIELDS = [
    "Apache", "Country", "HTTPServer", "IP", "RedirectLocation", "Title",
    "Bootstrap", "Email", "JQuery", "Meta-Author", "Open-Graph-Protocol",
    "PoweredBy", "Script", "Strict-Transport-Security", "UncommonHeaders",
    "WordPress", "X-Frame-Options"
]

OUTPUT_FILE = "whatweb_results.csv"
lock = Lock()  # thread-safe writes

def scan_domain(domain):
    result = subprocess.run(
        ["whatweb", "--log-json=-", domain],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Catch errors like "ERROR Opening"
    if "ERROR Opening:" in result.stdout or "ERROR Opening:" in result.stderr:
        return [{"Target": domain, **{field: "no data" for field in FIELDS}}]

    records = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line in ["[", "]", ","]:
            continue
        try:
            data = json.loads(line)
            record = {"Target": data.get("target")}
            plugins = data.get("plugins", {})
            for field in FIELDS:
                value = None
                if field in plugins:
                    if "string" in plugins[field]:
                        value = plugins[field]["string"]
                    elif "version" in plugins[field]:
                        value = plugins[field]["version"]
                record[field] = ", ".join(value) if value else None
            records.append(record)
        except json.JSONDecodeError:
            continue

    if not records:
        return [{"Target": domain, **{field: "no data" for field in FIELDS}}]

    return records

def merge_records(records):
    if not records:
        return None
    # Pick the richest record
    best_record = max(records, key=lambda r: sum(1 for v in r.values() if v and v not in [None, "None", "no data"]))
    # Fill missing values from others
    for rec in records:
        if rec is best_record:
            continue
        for k, v in rec.items():
            if not best_record.get(k) or best_record[k] in [None, "no data", "None"]:
                if v and v not in ["None", "no data"]:
                    best_record[k] = v
    return best_record

def process_domain(domain, writer):
    try:
        records = scan_domain(domain)
        merged_record = merge_records(records)
        if merged_record:
            with lock:
                writer.writerow(merged_record)
    except Exception as e:
        print(f"Error scanning {domain}: {e}")
        fallback_record = {"Target": domain, **{field: "no data" for field in FIELDS}}
        with lock:
            writer.writerow(fallback_record)

if __name__ == "__main__":
    # Load domains list
    df = pd.read_csv("nslookup.csv")
    domains = df["domain"].dropna().unique()  # ALL domains

    # Load already processed results
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing = pd.read_csv(OUTPUT_FILE)
            processed = set(existing["Target"].dropna().astype(str))
            print(f"Found {len(processed)} domains already processed. Skipping them...")
        except Exception:
            print("Could not read existing results file, starting fresh.")

    # Filter remaining domains
    remaining = [d for d in domains if d not in processed]
    print(f"{len(remaining)} domains left to process out of {len(domains)}")

    # Open file for appending
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Target"] + FIELDS)
        if not file_exists:
            writer.writeheader()

        with ThreadPoolExecutor(max_workers=25) as executor:
            futures = {
                executor.submit(process_domain, domain, writer): domain
                for domain in remaining
            }

            for i, future in enumerate(as_completed(futures), start=1):
                domain = futures[future]
                print(f"[{i}/{len(futures)}] Finished {domain}")
                f.flush()
