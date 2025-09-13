import subprocess
import requests
import csv
import re

FIELDS = [
    "domain", "ldhName", "status", "registration_date", "last_changed_date", "last_update_rdap_date",
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
    """Parse .lv WHOIS and map to CSV fields."""
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

    # Holder section
    match = re.search(r"\[Holder\](.*?)\n\n", text, re.S)
    if match:
        holder_section = match.group(1)
        country_match = re.search(r"Country:\s*(.+)", holder_section)
        if country_match:
            result["registrant_country"] = country_match.group(1).strip()

    # Nameservers
    ns_matches = re.findall(r"Nserver:\s*(.+)", text)
    if ns_matches:
        result["nameservers"] = ",".join([ns.strip() for ns in ns_matches if ns.strip() != "-"])

    return result

def domain_lookup(domain: str) -> dict:
    tld = domain.split('.')[-1].lower()
    rdap_servers = {"ch": "https://rdap.nic.ch/domain/", "li": "https://rdap.nic.ch/domain/",
                    "sj": "https://rdap.norid.no/domain/", "bv": "https://rdap.norid.no/domain/"}

    if tld in ["sj", "bv"]:
        return {"domain": domain, "status": "Reserved TLD", **{k: None for k in FIELDS if k not in ["domain","status"]}}

    if tld in ["ch", "li"]:
        try:
            resp = requests.get(rdap_servers[tld] + domain, timeout=10)
            if resp.status_code == 200:
                data = parse_rdap_json(resp.json())
                data["domain"] = domain
                return data
        except:
            pass
        return {"domain": domain, "status": f"RDAP lookup failed"}

    if tld == "lv":
        try:
            whois_text = subprocess.check_output(["whois", domain], text=True)
            data = parse_lv_whois(whois_text)
            data["domain"] = domain
            return data
        except Exception as e:
            return {"domain": domain, "status": f"WHOIS lookup failed ({e})"}

    return {"domain": domain, "status": "Unsupported TLD"}

def save_to_csv(domains, filename="domain_data.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for domain in domains:
            data = domain_lookup(domain)
            writer.writerow({k: data.get(k) for k in FIELDS})

# Example usage
if __name__ == "__main__":
    domain_list = ["example.ch", "example.li", "example.lv", "example.sj", "example.bv"]
    save_to_csv(domain_list)
    print("Saved domain data to domain_data.csv")
