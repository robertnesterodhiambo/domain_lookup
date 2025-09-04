import csv
import subprocess
import re

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

# Define schema (fixed columns)
COLUMNS = [
    "Domain", "DNS", "Registered", "Expires", "Registrar",
    "Registrar_website", "Registrar_email", "Registration_period",
    "VID", "DNSSEC", "Status",
    "Registrant_handle", "Registrant_name", "Registrant_address",
    "Registrant_postalcode", "Registrant_city", "Registrant_country",
    "Registrant_phone", "Nameservers"
]

def run_whois(domain):
    """Run whois command and return raw output"""
    try:
        result = subprocess.run(
            ["whois", domain],
            capture_output=True, text=True, timeout=20
        )
        return result.stdout
    except Exception as e:
        return f"ERROR: {e}"

def parse_whois(output):
    """Parse whois output into structured fields"""
    data = {col: "" for col in COLUMNS}

    patterns = {
        "Domain": r"Domain:\s+(.*)",
        "DNS": r"DNS:\s+(.*)",
        "Registered": r"Registered:\s+(.*)",
        "Expires": r"Expires:\s+(.*)",
        "Registrar": r"Registrar:\s+(.*)",
        "Registrar_website": r"Registrar website:\s+(.*)",
        "Registrar_email": r"Registrar email:\s+(.*)",
        "Registration_period": r"Registration period:\s+(.*)",
        "VID": r"VID:\s+(.*)",
        "DNSSEC": r"DNSSEC:\s+(.*)",
        "Status": r"Status:\s+(.*)",
        "Registrant_handle": r"Handle:\s+(.*)",
        "Registrant_name": r"Name:\s+(.*)",
        "Registrant_postalcode": r"Postalcode:\s+(.*)",
        "Registrant_city": r"City:\s+(.*)",
        "Registrant_country": r"Country:\s+(.*)",
        "Registrant_phone": r"Phone:\s+(.*)",
    }

    for field, pat in patterns.items():
        match = re.search(pat, output, re.IGNORECASE)
        if match:
            data[field] = match.group(1).strip()

    # Collect multiple addresses (concatenate with ;)
    addresses = re.findall(r"Address:\s+(.*)", output, re.IGNORECASE)
    if addresses:
        data["Registrant_address"] = "; ".join([a.strip() for a in addresses])

    # Collect nameservers (accept both Hostname: and Nameserver:)
    ns_matches = re.findall(r"(?:Hostname|Nameserver):\s+(.*)", output, re.IGNORECASE)
    if ns_matches:
        data["Nameservers"] = "; ".join([n.strip() for n in ns_matches])

    return data

def main():
    with open(INPUT_CSV, newline="") as infile, \
         open(OUTPUT_CSV, "w", newline="") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=COLUMNS)
        writer.writeheader()

        for i, row in enumerate(reader):
            if i >= 5:
                break

            domain = row["domain"]
            print(f"\n>>> Running whois for: {domain}")

            whois_output = run_whois(domain)
            print(whois_output)

            parsed = parse_whois(whois_output)

            # Save row immediately
            writer.writerow(parsed)
            outfile.flush()
            print(f"✅ Saved to CSV: {domain}")

if __name__ == "__main__":
    main()
