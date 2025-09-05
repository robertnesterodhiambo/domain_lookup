import pandas as pd
import subprocess
import csv
import os
import re
import time

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

COLUMNS = [
    "Domain", "DNS", "Registered", "Expires", "Registrar",
    "Registration_period", "VID", "DNSSEC", "Status",
    "Registrant_handle", "Registrant_name", "Registrant_address",
    "Registrant_postalcode", "Registrant_city", "Registrant_country",
    "Registrant_phone", "Nameservers", "Registrar_website", "Registrar_email"
]

WHOIS_URL_TEMPLATE = "https://www.whois.lt/en/whois/{domain}"

# Your proxy credentials
USERNAME = "geonode_DrXb2XNsHm-type-residential"
PASSWORD = "f232262f-0f34-400c-a7a6-84d1ce423302"
PROXY_HOST = "92.204.164.15:9000"
PROXY = f"http://{USERNAME}:{PASSWORD}@{PROXY_HOST}"

def run_curl_whois(domain):
    """Fetch WHOIS page via curl through your authenticated proxy"""
    try:
        result = subprocess.run(
            [
                "curl", "-s", "-x", PROXY,
                WHOIS_URL_TEMPLATE.format(domain=domain)
            ],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.stdout
    except Exception as e:
        print(f"Error fetching WHOIS for {domain}: {e}")
        return ""

def parse_whois_text(raw_text):
    data = {col: "" for col in COLUMNS}
    clean_text = re.sub(r"<[^>]+>", "", raw_text)

    for line in clean_text.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("Domain:"):
            data["Domain"] = line.split(":", 1)[1].strip()
        elif line.startswith("Status:"):
            data["Status"] = line.split(":", 1)[1].strip()
        elif line.startswith("Registered:"):
            data["Registered"] = line.split(":", 1)[1].strip()
        elif line.startswith("Expires:"):
            data["Expires"] = line.split(":", 1)[1].strip()
        elif line.startswith("Registrar:"):
            data["Registrar"] = line.split(":", 1)[1].strip()
        elif line.startswith("Registrar website:"):
            data["Registrar_website"] = line.split(":", 1)[1].strip()
        elif line.startswith("Registrar email:"):
            data["Registrar_email"] = line.split(":", 1)[1].strip()
        elif line.startswith("Address:"):
            if data["Registrant_address"]:
                data["Registrant_address"] += "; " + line.split(":", 1)[1].strip()
            else:
                data["Registrant_address"] = line.split(":", 1)[1].strip()
        elif line.startswith("Postalcode:"):
            data["Registrant_postalcode"] = line.split(":", 1)[1].strip()
        elif line.startswith("City:"):
            data["Registrant_city"] = line.split(":", 1)[1].strip()
        elif line.startswith("Country:"):
            data["Registrant_country"] = line.split(":", 1)[1].strip()
        elif line.startswith("Phone:"):
            data["Registrant_phone"] = line.split(":", 1)[1].strip()
        elif line.startswith("Nameserver:") or line.startswith("Hostname:") or line.startswith("nserver:"):
            if data["Nameservers"]:
                data["Nameservers"] += "; " + line.split(":", 1)[1].strip()
            else:
                data["Nameservers"] = line.split(":", 1)[1].strip()

    return data

def main():
    df = pd.read_csv(INPUT_CSV)

    processed = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "Domain" in row and row["Domain"]:
                    processed.add(row["Domain"].strip().lower())

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

        print(f"Fetching WHOIS for {domain} via proxy...")
        raw_output = run_curl_whois(domain)

        parsed = parse_whois_text(raw_output)

        combined = {**row.to_dict(), **parsed}
        writer.writerow(combined)
        out_file.flush()

        processed_count += 1
        time.sleep(1)

    out_file.close()
    print("\nSummary:")
    print(f"✅ {processed_count} new domains processed")
    print(f"⏭️ {skipped_count} domains skipped (already collected)")

if __name__ == "__main__":
    main()
