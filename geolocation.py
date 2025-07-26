import pandas as pd
import requests
import time
import csv
import random
import os

input_file = 'violations.csv'
output_file = 'complete.csv'

proxy_credentials = [
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302"
]


# Prepare proxy list
proxy_list = []
for cred in proxy_credentials:
    host, port, user, password = cred.split(":")
    proxy_url = f"http://{user}:{password}@{host}:{port}"
    proxy_list.append({
        "http": proxy_url,
        "https": proxy_url
    })

geo_fields = ['city', 'region', 'country', 'loc', 'org', 'postal', 'timezone']
ip_cache = {}

# Track already processed domains
processed_domains = set()
rows_collected = 0
rows_skipped = 0

# Load already collected domains from existing complete.csv
if os.path.exists(output_file):
    try:
        existing_df = pd.read_csv(output_file, usecols=['domain'])
        processed_domains = set(existing_df['domain'].dropna().astype(str))
        rows_collected = len(processed_domains)
        print(f"✅ Already collected: {rows_collected} rows")
    except Exception as e:
        print(f"⚠️ Could not read existing {output_file}: {e}")

def fetch_ipinfo(ip):
    try:
        return requests.get(f'https://ipinfo.io/{ip}/json', timeout=5)
    except:
        return None

def fetch_ipinfo_with_proxies(ip):
    for proxy in random.sample(proxy_list, len(proxy_list)):
        try:
            resp = requests.get(f'https://ipinfo.io/{ip}/json', timeout=10, proxies=proxy)
            if resp.status_code == 200:
                return resp
        except:
            pass
    return None

# Open output CSV in append mode
with open(output_file, mode='a', newline='', encoding='utf-8') as out_csv:
    writer = None
    file_empty = os.stat(output_file).st_size == 0

    for chunk in pd.read_csv(input_file, chunksize=100, quotechar='"', on_bad_lines='skip'):
        for _, row in chunk.iterrows():
            domain = str(row.get('domain', '')).strip()
            if not domain:
                continue
            if domain in processed_domains:
                rows_skipped += 1
                continue

            nslookupA = row.get('nslookupA', '')
            ip_list = [ip.strip() for ip in str(nslookupA).split('|') if ip.strip()] if pd.notna(nslookupA) else []

            geo_data = {field: '' for field in geo_fields}

            for ip in ip_list:
                if ip in ip_cache:
                    geo_data = ip_cache[ip]
                    break

                # Try direct fetch
                resp = fetch_ipinfo(ip)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    geo_data = {field: data.get(field, '') for field in geo_fields}
                    ip_cache[ip] = geo_data
                    break

                # Try with proxies
                resp = fetch_ipinfo_with_proxies(ip)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    geo_data = {field: data.get(field, '') for field in geo_fields}
                    ip_cache[ip] = geo_data
                    break

                # Cache blank result if all fail
                ip_cache[ip] = geo_data

                time.sleep(0.5)

            # Add geo fields to row
            for field in geo_fields:
                row[field] = geo_data[field]

            # Write header once if needed
            if writer is None:
                writer = csv.DictWriter(out_csv, fieldnames=row.index)
                if file_empty:
                    writer.writeheader()
                writer.writeheader() if file_empty else None

            writer.writerow(row.to_dict())
            processed_domains.add(domain)
            rows_collected += 1

print(f"✅ Collected now: {rows_collected} rows total")
print(f"⏩ Skipped duplicates: {rows_skipped} rows")
