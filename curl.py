#!/usr/bin/env python3
import pandas as pd
import subprocess
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"
MAX_WORKERS = 200  # 999 is too high, system will choke

# CSV output columns
CSV_COLUMNS = [
    "Domain",
    "Status",
    "Registered",
    "Expires",
    "Registrar",
    "Registrar website",
    "Registrar email",
    "Contact organization",
    "Contact email",
    "Nameserver1",
    "Nameserver2"
]

# Thread lock for CSV writing
write_lock = threading.Lock()

def run_whois(domain):
    """Run WHOIS through proxychains4 using the environment-configured proxies."""
    try:
        print(f"   🌐 Fetching via proxychains4...")

        cmd = ["proxychains4", "whois", domain]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=40
        )

        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
        return None
    except Exception as e:
        print(f"⚠️ Exception fetching WHOIS for {domain}: {e}")
        return None

def parse_whois(raw, domain):
    if not raw or ("domain" not in raw.lower() and "registrar" not in raw.lower()):
        return None

    data = {col: None for col in CSV_COLUMNS}
    data["Domain"] = domain
    nameservers = []

    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("%"):
            continue
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key, value = key.strip().lower(), value.strip()

        if key in ("status", "domain status", "state"):
            data["Status"] = value
        elif key in ("registered", "registration time", "created", "creation date", "domain registration date"):
            data["Registered"] = value
        elif key in ("expires", "expiry date", "expire date", "registry expiry date", "expiration time"):
            data["Expires"] = value
        elif key.startswith("registrar url") or key.startswith("registrar website"):
            data["Registrar website"] = value
        elif key.startswith("registrar email"):
            data["Registrar email"] = value
        elif key.startswith("registrar"):
            data["Registrar"] = value
        elif "contact organization" in key or "organization" in key:
            data["Contact organization"] = value
        elif "contact email" in key or (key == "email" and "registrar" not in key):
            data["Contact email"] = value
        elif key.startswith("nameserver") or key.startswith("nserver"):
            nameservers.append(value)

    if nameservers:
        data["Nameserver1"] = nameservers[0]
        if len(nameservers) > 1:
            data["Nameserver2"] = nameservers[1]

    return data

def process_domain(domain):
    print(f"\nFetching WHOIS for {domain}...")
    raw = run_whois(domain)

    if not raw:
        print(f"❌ No WHOIS data found for {domain}.")
        return None

    parsed = parse_whois(raw, domain)
    if not parsed:
        print(f"⚠️ Could not parse WHOIS for {domain}. Showing first 10 lines:\n")
        print("\n".join(raw.splitlines()[:10]))
        return None

    with write_lock:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writerow(parsed)

    print(f"✔ Saved {domain} to {OUTPUT_CSV}")
    return parsed

def main():
    # Read already processed domains to skip
    processed_domains = set()
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        processed_domains = set(existing_df["Domain"].dropna().unique())
        print(f"ℹ️ Skipping {len(processed_domains)} already processed domains.")

    df = pd.read_csv(INPUT_CSV)
    domains = [d for d in df["domain"].dropna().unique() if d not in processed_domains]

    # If output file doesn't exist, write header
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_domain, domain): domain for domain in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"⚠️ Error processing {domain}: {e}")

if __name__ == "__main__":
    main()
