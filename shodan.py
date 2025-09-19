import requests
import pandas as pd
import csv
import ipaddress

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

# open input file
df = pd.read_csv(INPUT_FILE)

# prepare output CSV with headers
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["nslookup", "ip", "cpes", "hostnames", "ports", "tags", "vulns"])
    writer.writeheader()

# iterate rows
for idx, row in df.iterrows():
    nslookup_val = str(row.get("nslookup", ""))
    result = {"nslookup": nslookup_val, "ip": "no data", "cpes": "no data", "hostnames": "no data",
              "ports": "no data", "tags": "no data", "vulns": "no data"}

    if nslookup_val.lower() != "no data" and nslookup_val.strip():
        ips = [x.strip() for x in nslookup_val.split("|") if is_valid_ip(x.strip())]
        for ip in ips:
            data = query_shodan(ip)
            if data:
                result.update(data)
                break

    # append to CSV immediately
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=result.keys())
        writer.writerow(result)

    print(f"Row {idx+1} processed and saved.")

print("✅ All rows processed and saved.")
