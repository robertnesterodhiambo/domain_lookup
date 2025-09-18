import subprocess
import json
import pandas as pd
import csv

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
    return records

def merge_records(records):
    if not records:
        return None
    
    best_record = max(records, key=lambda r: sum(1 for v in r.values() if v and v != "None"))
    
    for rec in records:
        if rec is best_record:
            continue
        for k, v in rec.items():
            if not best_record.get(k) and v:
                best_record[k] = v
    return best_record

if __name__ == "__main__":
    # Load domains
    df = pd.read_csv("combined_nslookup.csv")
    domains = df["domain"].dropna().unique()[:100]  # first 100 unique domains
    
    # Prepare output file with headers if not exists
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Target"] + FIELDS)
        writer.writeheader()
    
    for i, domain in enumerate(domains, start=1):
        print(f"[{i}/{len(domains)}] Scanning {domain} ...")
        try:
            records = scan_domain(domain)
            merged_record = merge_records(records)
            if merged_record:
                with open(OUTPUT_FILE, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=["Target"] + FIELDS)
                    writer.writerow(merged_record)
        except Exception as e:
            print(f"Error scanning {domain}: {e}")
