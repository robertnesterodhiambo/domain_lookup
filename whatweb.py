import subprocess
import json
import pandas as pd
import csv
import os

FIELDS = [
    "Apache", "Country", "HTTPServer", "IP", "RedirectLocation", "Title",
    "Bootstrap", "Email", "JQuery", "Meta-Author", "Open-Graph-Protocol",
    "PoweredBy", "Script", "Strict-Transport-Security", "UncommonHeaders",
    "WordPress", "X-Frame-Options"
]

OUTPUT_FILE = "whatweb_results.csv"

def scan_domain(domain):
    result = subprocess.run(
        ["whatweb", "--log-json=-", domain],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Check for error like "ERROR Opening: ..."
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
        # if no usable data at all
        return [{"Target": domain, **{field: "no data" for field in FIELDS}}]

    return records

def merge_records(records):
    if not records:
        return None
    
    # Prefer the record with most filled fields
    best_record = max(records, key=lambda r: sum(1 for v in r.values() if v and v != "None" and v != "no data"))
    
    # Merge missing values from other records
    for rec in records:
        if rec is best_record:
            continue
        for k, v in rec.items():
            if not best_record.get(k) or best_record[k] in [None, "no data"]:
                if v and v not in ["None", "no data"]:
                    best_record[k] = v
    return best_record

if __name__ == "__main__":
    # Load domains
    df = pd.read_csv("combined_nslookup.csv")
    domains = df["domain"].dropna().unique()[:100]  # first 100 unique domains

    # Load already processed targets if file exists
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing = pd.read_csv(OUTPUT_FILE)
            processed = set(existing["Target"].dropna().astype(str))
            print(f"Found {len(processed)} domains already processed. Skipping them...")
        except Exception:
            print("Could not read existing results file, starting fresh.")

    # Open output file (append mode if exists, else write with header)
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Target"] + FIELDS)
        if not file_exists:
            writer.writeheader()

        for i, domain in enumerate(domains, start=1):
            if domain in processed:
                print(f"[{i}/{len(domains)}] Skipping {domain} (already collected)")
                continue

            print(f"[{i}/{len(domains)}] Scanning {domain} ...")
            try:
                records = scan_domain(domain)
                merged_record = merge_records(records)
                if merged_record:
                    writer.writerow(merged_record)
                    f.flush()
            except Exception as e:
                print(f"Error scanning {domain}: {e}")
                # Write "no data" row if anything goes wrong
                fallback_record = {"Target": domain, **{field: "no data" for field in FIELDS}}
                writer.writerow(fallback_record)
                f.flush()
