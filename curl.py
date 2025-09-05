#!/usr/bin/env python3
import pandas as pd
import subprocess
import csv

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

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

def run_whois(domain):
    try:
        # Force WHOIS through proxy using proxychains
        cmd = ["proxychains4", "whois", domain]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=40
        )
        if result.returncode == 0:
            return result.stdout
        else:
            return None
    except Exception:
        return None

def parse_whois(raw, domain):
    """Extract structured fields from WHOIS text."""
    if not raw:
        return {col: None for col in CSV_COLUMNS}

    data = {
        "Domain": domain,
        "Status": None,
        "Registered": None,
        "Expires": None,
        "Registrar": None,
        "Registrar website": None,
        "Registrar email": None,
        "Contact organization": None,
        "Contact email": None,
        "Nameserver1": None,
        "Nameserver2": None,
    }

    nameservers = []

    for line in raw.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue  # skip invalid lines

        key, value = line.split(":", 1)
        key, value = key.strip().lower(), value.strip()

        if key.startswith("status"):
            data["Status"] = value
        elif key.startswith("registered"):
            data["Registered"] = value
        elif key.startswith("expires"):
            data["Expires"] = value
        elif key.startswith("registrar website"):
            data["Registrar website"] = value
        elif key.startswith("registrar email"):
            data["Registrar email"] = value
        elif key.startswith("registrar"):
            data["Registrar"] = value
        elif key.startswith("contact organization"):
            data["Contact organization"] = value
        elif key.startswith("contact email"):
            data["Contact email"] = value
        elif key.startswith("nameserver"):
            nameservers.append(value)

    if nameservers:
        data["Nameserver1"] = nameservers[0]
        if len(nameservers) > 1:
            data["Nameserver2"] = nameservers[1]

    return data

def main():
    df = pd.read_csv(INPUT_CSV)

    # Write header first
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

    for domain in df["domain"]:
        print(f"\nFetching WHOIS for {domain} through proxy...")
        raw = run_whois(domain)
        if raw:
            print("✅ WHOIS output:\n")
            print(raw)
        else:
            print("❌ No WHOIS data found.")

        parsed = parse_whois(raw, domain)

        # Append immediately to CSV
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writerow(parsed)

        print(f"✔ Saved {domain} to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
