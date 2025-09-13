import subprocess
import pandas as pd
import re
import csv
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

FIELDS = [
    "domain", "count", "tld", "rdap", "rdap_link",
    "status", "ldhName", "registration_date", "last_changed_date", "last_update_rdap_date",
    "registrant_name", "registrant_email", "admin_name", "admin_email", "tech_name", "tech_email",
    "registrar_name", "registrar_email", "registrar_addr", "registrar_city", "registrar_region",
    "registrar_postalcode", "registrar_country", "reseller_name", "nameservers", "secureDNS_delegationSigned"
]

MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds
THREADS = 5

csv_lock = Lock()  # ensure no double writes

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
            registrar_addr_fields = {
                "registrar_addr": None,
                "registrar_city": None,
                "registrar_region": None,
                "registrar_postalcode": None,
                "registrar_country": None
            }

            if vcard and len(vcard) == 2:
                for item in vcard[1]:
                    if item[0] == "fn":
                        name = item[3]
                    elif item[0] == "email":
                        email = item[3]
                    elif item[0] == "adr":
                        adr_list = item[3]
                        registrar_addr_fields["registrar_addr"] = adr_list[2] if len(adr_list) > 2 else None
                        registrar_addr_fields["registrar_city"] = adr_list[3] if len(adr_list) > 3 else None
                        registrar_addr_fields["registrar_region"] = adr_list[4] if len(adr_list) > 4 else None
                        registrar_addr_fields["registrar_postalcode"] = adr_list[5] if len(adr_list) > 5 else None
                        registrar_addr_fields["registrar_country"] = adr_list[6] if len(adr_list) > 6 else None

            if "registrant" in roles:
                result["registrant_name"], result["registrant_email"] = name, email
            elif "administrative" in roles:
                result["admin_name"], result["admin_email"] = name, email
            elif "technical" in roles:
                result["tech_name"], result["tech_email"] = name, email
            elif "registrar" in roles:
                result["registrar_name"] = name
                result["registrar_email"] = email
                for k, v in registrar_addr_fields.items():
                    result[k] = v
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

def proxy_rdap_get(url: str, timeout=15):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = subprocess.run(
                ["proxychains4", "curl", "-s", url],
                capture_output=True, text=True, timeout=timeout
            )
            if result.returncode == 0 and result.stdout:
                return json.loads(result.stdout)
        except Exception:
            pass
        print(f"RDAP retry {attempt} failed for {url}, retrying...")
        time.sleep(RETRY_DELAY)
    return None

def proxy_whois(domain: str, timeout=15):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = subprocess.run(
                ["proxychains4", "whois", domain],
                capture_output=True, text=True, timeout=timeout
            )
            if result.returncode == 0:
                return result.stdout
        except Exception:
            pass
        print(f"WHOIS retry {attempt} failed for {domain}, retrying...")
        time.sleep(RETRY_DELAY)
    return None

def domain_lookup(domain: str, tld: str) -> dict:
    rdap_servers = {
        "ch": "https://rdap.nic.ch/domain/",
        "li": "https://rdap.nic.ch/domain/",
        "sj": "https://rdap.norid.no/domain/",
        "bv": "https://rdap.norid.no/domain/"
    }

    if tld in ["sj", "bv"]:
        return {"status": "Reserved TLD"}

    if tld in ["ch", "li"]:
        data = proxy_rdap_get(rdap_servers[tld] + domain)
        if data:
            return parse_rdap_json(data)
        else:
            return {"status": "RDAP lookup failed after 3 retries"}

    if tld == "lv":
        whois_text = proxy_whois(domain)
        if whois_text:
            return parse_lv_whois(whois_text)
        else:
            return {"status": "WHOIS lookup failed after 3 retries"}

    return {"status": "Unsupported TLD"}

def process_domain(row, output_csv):
    domain = row['domain']
    tld = row['tld'].lower()
    print(f"Processing {domain} via proxychains4...")
    data = domain_lookup(domain, tld)
    data['domain'] = domain
    data['count'] = row.get('count', None)
    data['tld'] = tld
    data['rdap'] = row.get('rdap', None)
    data['rdap_link'] = row.get('rdap_link', None)

    with csv_lock:
        with open(output_csv, "a", newline="", encoding="utf-8", buffering=1) as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if f.tell() == 0:
                writer.writeheader()
            writer.writerow({k: data.get(k) for k in FIELDS})
            f.flush()
    return domain

def main(input_csv="data_rdap.csv", output_csv="data_rdap_parsed.csv"):
    df = pd.read_csv(input_csv)

    # Already processed domains
    existing_domains = set()
    if os.path.exists(output_csv):
        existing_df = pd.read_csv(output_csv)
        existing_domains = set(existing_df['domain'].astype(str).tolist())

    rows_to_process = [row for _, row in df.iterrows() if row['domain'] not in existing_domains]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(process_domain, row, output_csv) for row in rows_to_process]
        for future in as_completed(futures):
            domain = future.result()
            print(f"Finished: {domain}")

    print(f"All domains processed and appended to {output_csv}")

if __name__ == "__main__":
    main()
