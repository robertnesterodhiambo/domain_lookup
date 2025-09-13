import subprocess
import requests
import pandas as pd
import re
import csv

FIELDS = [
    "domain", "count", "tld", "rdap", "rdap_link",
    "status", "ldhName", "registration_date", "last_changed_date", "last_update_rdap_date",
    "registrant_name", "registrant_email", "admin_name", "admin_email", "tech_name", "tech_email",
    "registrar_name", "registrar_addr", "registrar_city", "registrar_region", "registrar_postalcode",
    "registrar_country", "reseller_name", "nameservers", "secureDNS_delegationSigned"
]

def parse_rdap_json(data: dict) -> dict:
    result = {k: None for k in FIELDS}
    result["ldhName"] = data.get("ldhName")
    result["status"] = ",".join(data.get("status", [])) if data.get("status") else None
    if data.get("events"):
        result["registration_date"] = data["events"][0].get("eventDate")
        if len(data["events"]) > 1:
            result["last_changed_date"] = data["events"][1].get("eventDate")
        result["last_update_rdap_date"] = data["events"][0].get("eventDate")
    if "nameservers" in data:
        result["nameservers"] = ",".join(ns.get("ldhName") for ns in data["nameservers"] if ns.get("ldhName"))
    if "entities" in data:
        for entity in data["entities"]:
            roles = entity.get("roles", [])
            vcard = entity.get("vcardArray", [])
            name, email = None, None
            if vcard and len(vcard) == 2:
                for item in vcard[1]:
                    if item[0] == "fn":
                        name = item[3]
                    elif item[0] == "email":
                        email = item[3]
            if "registrant" in roles:
                result["registrant_name"], result["registrant_email"] = name, email
            elif "administrative" in roles:
                result["admin_name"], result["admin_email"] = name, email
            elif "technical" in roles:
                result["tech_name"], result["tech_email"] = name, email
            elif "registrar" in roles:
                result["registrar_name"] = name
                adr = entity.get("adr", {})
                if adr:
                    result["registrar_addr"] = adr.get("streetAddress")
                    result["registrar_city"] = adr.get("locality")
                    result["registrar_region"] = adr.get("region")
                    result["registrar_postalcode"] = adr.get("postalCode")
                    result["registrar_country"] = adr.get("countryName")
            elif "reseller" in roles:
                result["reseller_name"] = name
    result["secureDNS_delegationSigned"] = data.get("secureDNS", {}).get("delegationSigned")
    return result

def parse_lv_whois(text: str) -> dict:
    result = {k: None for k in FIELDS}
    match = re.search(r"Domain:\s*(.+)", text)
    if match:
        result["ldhName"] = match.group(1).strip()
    match = re.search(r"Status:\s*(.+)", text)
    if match:
        result["status"] = match.group(1).strip()
    match = re.search(r"Updated:\s*(.+)", text)
    if match:
        result["last_update_rdap_date"] = match.group(1).strip()
    ns_matches = re.findall(r"Nserver:\s*(.+)", text)
    if ns_matches:
        result["nameservers"] = ",".join([ns.strip() for ns in ns_matches if ns.strip() != "-"])
    return result

def proxy_rdap_get(url, timeout=10):
    """Use proxychains4 for HTTP requests."""
    try:
        result = subprocess.run(
            ["proxychains4", "curl", "-s", url],
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode == 0 and result.stdout:
            import json
            return json.loads(result.stdout)
    except Exception as e:
        print(f"Proxy RDAP request failed: {e}")
    return None

def domain_lookup(domain: str, tld: str) -> dict:
    rdap_servers = {"ch": "https://rdap.nic.ch/domain/", "li": "https://rdap.nic.ch/domain/",
                    "sj": "https://rdap.norid.no/domain/", "bv": "https://rdap.norid.no/domain/"}

    if tld in ["sj", "bv"]:
        return {"status": "Reserved TLD"}

    if tld in ["ch", "li"]:
        data = proxy_rdap_get(rdap_servers[tld] + domain)
        if data:
            return parse_rdap_json(data)
        else:
            return {"status": "RDAP lookup failed"}

    if tld == "lv":
        try:
            whois_text = subprocess.check_output(["whois", domain], text=True)
            return parse_lv_whois(whois_text)
        except:
            return {"status": "WHOIS lookup failed"}

    return {"status": "Unsupported TLD"}

def main(input_csv="data_rdap.csv", output_csv="data_rdap_parsed.csv"):
    df = pd.read_csv(input_csv)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()

        for _, row in df.iterrows():
            domain = row['domain']
            tld = row['tld'].lower()
            print(f"Processing {domain} via proxychains4...")
            data = domain_lookup(domain, tld)

            # Include original CSV columns
            data['domain'] = domain
            data['count'] = row.get('count', None)
            data['tld'] = tld
            data['rdap'] = row.get('rdap', None)
            data['rdap_link'] = row.get('rdap_link', None)

            # Write row immediately
            writer.writerow({k: data.get(k) for k in FIELDS})

    print(f"Saved parsed data to {output_csv}")

if __name__ == "__main__":
    main()
