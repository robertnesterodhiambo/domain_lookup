import requests
import pandas as pd
import csv
import ipaddress
import os

INPUT_FILE = "combined_nslookup.csv"
OUTPUT_FILE = "shodan_results.csv"
SHODAN_URL = "https://internetdb.shodan.io/"

# flatten helper
def flatten_list(data):
    if isinstance(data, list):
        return ";".join(map(str, data))
    return data if data else "no data"

# check valid ip
def is_valid_ip(ip):
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False

# query shodan
def query_shodan(ip):
    try:
        r = requests.get(SHODAN_URL + ip, timeout=10)
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            return {
                "ip": data.get("ip", "no data"),
                "cpes": flatten_list(data.get("cpes", [])),
                "hostnames": flatten_list(data.get("hostnames", [])),
                "ports": flatten_list(data.get("ports", [])),
                "tags": flatten_list(data.get("tags", [])),
                "vulns": flatten_list(data.get("vulns", [])),
            }
    except Exception:
        return None
    return None

# load input
df = pd.read_csv(INPUT_FILE)

# check if output exists, load processed keys
processed = set()
if os.path.exists(OUTPUT_FILE):
    try:
        existing = pd.read_csv(OUTPUT_FILE)
        for _, row in existing.iterrows():
            key = (str(row.get("domain", "")).strip(), str(row.get("nslookup", "")).strip())
            processed.add(key)
    except Exception:
        pass

# open output file (append mode)
with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["domain", "nslookup", "ip", "cpes", "hostnames", "ports", "tags", "vulns"]
    )
    # write header only if file is empty
    if os.stat(OUTPUT_FILE).st_size == 0:
        writer.writeheader()

    # iterate rows
    for idx, row in df.iterrows():
        nslookup_val = str(row.get("nslookup", ""))
        domain_val = str(row.get("domain", "no data"))
        key = (domain_val.strip(), nslookup_val.strip())

        if key in processed:
            print(f"Row {idx+1} skipped (already collected).")
            continue

        result = {
            "domain": domain_val,
            "nslookup": nslookup_val,
            "ip": "no data",
            "cpes": "no data",
            "hostnames": "no data",
            "ports": "no data",
            "tags": "no data",
            "vulns": "no data"
        }

        if nslookup_val.lower() != "no data" and nslookup_val.strip():
            ips = [x.strip() for x in nslookup_val.split("|") if is_valid_ip(x.strip())]
            for ip in ips:
                data = query_shodan(ip)
                if data:
                    result.update(data)
                    break

        # append row immediately
        writer.writerow(result)
        f.flush()

        print(f"Row {idx+1} processed and saved.")

print("✅ All new rows processed and saved.")
