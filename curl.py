#!/usr/bin/env python3
import pandas as pd
import subprocess
import csv
import os

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

# Fixed CSV columns
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
    "Nameserver2",
    "Nameserver3",
    "Nameserver4",
]

# Mapping from WHOIS keys to CSV column names
KEY_MAPPING = {
    "Domain Name": "Domain",
    "Status": "Status",
    "Creation Date": "Registered",
    "Registry Expiry Date": "Expires",
    "Registrar": "Registrar",
    "Registrar URL": "Registrar website",
    "Registrar Abuse Contact Email": "Registrar email",
    "Registrant Organization": "Contact organization",
    "Registrant Email": "Contact email",
    "Name Server": "Nameserver",  # handled separately
}


def fetch_whois(domain):
    print(f"Fetching WHOIS for {domain}...")

    try:
        # Run whois via proxychains4 so it uses your SOCKS5 proxy
        result = subprocess.run(
            ["proxychains4", "whois", domain],
            capture_output=True,
            text=True,
            check=True
        )
        whois_text = result.stdout
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Error fetching {domain}: {e}")
        return None

    data_dict = {}
    nameservers = []

    # Parse WHOIS text line by line
    for line in whois_text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key.lower().startswith("name server"):
            nameservers.append(value)
        else:
            csv_key = KEY_MAPPING.get(key)
            if csv_key:
                data_dict[csv_key] = value

    # Add up to 4 nameservers
    for i in range(4):
        col_name = f"Nameserver{i+1}"
        data_dict[col_name] = nameservers[i] if i < len(nameservers) else "N/A"

    # Ensure all fixed columns exist
    for col in CSV_COLUMNS:
        if col not in data_dict:
            data_dict[col] = "N/A"

    return data_dict


if __name__ == "__main__":
    # Load input CSV
    df = pd.read_csv(INPUT_CSV)

    # Normalize column name
    if "domain" not in df.columns and "Domain" in df.columns:
        df.rename(columns={"Domain": "domain"}, inplace=True)

    domains = df["domain"].head(100).tolist()

    # Track already processed domains
    processed = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed.add(row["Domain"])

    # Open CSV in append mode
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if os.stat(OUTPUT_CSV).st_size == 0:
            writer.writeheader()

        for domain in domains:
            if domain in processed:
                print(f"Skipping already processed domain: {domain}")
                continue

            data = fetch_whois(domain)
            if data:
                writer.writerow(data)
                f.flush()
                print(f"✅ Saved WHOIS for {domain}")
