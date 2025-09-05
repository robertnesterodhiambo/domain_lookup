import pandas as pd
import subprocess
import csv
import os
import re
import time

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

# Columns we always want
COLUMNS = [
    "Domain", "DNS", "Registered", "Expires", "Registrar",
    "Registration_period", "VID", "DNSSEC", "Status",
    "Registrant_handle", "Registrant_name", "Registrant_address",
    "Registrant_postalcode", "Registrant_city", "Registrant_country", "Registrant_phone",
    "Nameservers", "Registrar_website", "Registrar_email"
]

def run_whois(domain):
    """Run whois command and return raw output"""
    try:
        result = subprocess.run(["curl", f"https://www.whois.com/whois/{domain}"], capture_output=True, text=True, timeout=20)
        return result.stdout
    except Exception as e:
        print(f"Error running whois for {domain}: {e}")
        return ""

def parse_whois_output(output):
    """Extract relevant fields from whois output"""
    data = {col: "" for col in COLUMNS}

    # Generic patterns
    patterns = {
        "Domain": r"Domain:\s*(.+)",
        "DNS": r"DNS:\s*(.+)",
        "Registered": r"Registered:\s*(.+)",
        "Expires": r"Expires:\s*(.+)",
        "Registrar": r"Registrar:\s*(.+)",
        "Registration_period": r"Registration period:\s*(.+)",
        "VID": r"VID:\s*(.+)",
        "DNSSEC": r"DNSSEC:\s*(.+)",
        "Status": r"Status:\s*(.+)",
        "Registrant_handle": r"Handle:\s*(.+)",
        "Registrant_name": r"Name:\s*(.+)",
        "Registrant_postalcode": r"Postalcode:\s*(.+)",
        "Registrant_city": r"City:\s*(.+)",
        "Registrant_country": r"Country:\s*(.+)",
        "Registrant_phone": r"Phone:\s*(.+)",
        "Registrar_website": r"Registrar website:\s*(.+)",
        "Registrar_email": r"Registrar email:\s*(.+)"
    }

    for field, pattern in patterns.items():
        match = re.search(pattern, output, re.IGNORECASE)
        if match:
            data[field] = match.group(1).strip()

    # Collect multiple Address lines
    addresses = re.findall(r"Address:\s*(.+)", output, re.IGNORECASE)
    if addresses:
        data["Registrant_address"] = "; ".join([a.strip() for a in addresses])

    # Collect multiple Hostname / Nameserver / nserver
    nameservers = re.findall(r"(?:Hostname|Nameserver|nserver):\s*(.+)", output, re.IGNORECASE)
    if nameservers:
        data["Nameservers"] = "; ".join([ns.strip() for ns in nameservers])

    return data

def main():
    # Read input
    df = pd.read_csv(INPUT_CSV)

    # Track already processed domains
    processed = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "Domain" in row and row["Domain"]:
                    processed.add(row["Domain"].strip().lower())

    # Prepare output file
    write_header = not os.path.exists(OUTPUT_CSV)
    out_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=df.columns.tolist() + COLUMNS)
    if write_header:
        writer.writeheader()

    skipped_count = 0
    processed_count = 0

    for _, row in df.iterrows():
        domain = row["domain"].strip()
        if domain.lower() in processed:
            skipped_count += 1
            print(f"Skipping {domain}, already collected.")
            continue

        print(f"Running whois for {domain}...")
        raw_output = run_whois(domain)
        print(raw_output)  # print to terminal

        parsed = parse_whois_output(raw_output)

        # Merge input row + parsed data
        combined = {**row.to_dict(), **parsed}
        writer.writerow(combined)
        out_file.flush()  # save immediately

        processed_count += 1

        # Small delay to avoid rate limits
        time.sleep(1)

    out_file.close()

    print("\nSummary:")
    print(f"✅ {processed_count} new domains processed")
    print(f"⏭️ {skipped_count} domains skipped (already collected)")

if __name__ == "__main__":
    main()
