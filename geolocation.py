import pandas as pd
import requests
import time
import csv
import random
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

input_file = 'violations.csv'
output_file = 'complete.csv'

proxy_credentials = [
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302"
] * 10

proxy_list = []
for cred in proxy_credentials:
    host, port, user, password = cred.split(":")
    proxy_url = f"http://{user}:{password}@{host}:{port}"
    proxy_list.append({"http": proxy_url, "https": proxy_url})

geo_fields = ['city', 'region', 'country', 'loc', 'org', 'postal', 'timezone']
ip_cache = {}
cache_lock = Lock()
write_lock = Lock()

processed_domains = set()
rows_collected = 0
rows_skipped = 0

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
            continue
    return None

def process_row(row_number, row, writer, processed_domains):
    global rows_collected, rows_skipped
    domain = str(row.get('domain', '')).strip()
    print(f"📄 Processing row {row_number} - domain: {domain}")

    if not domain or domain in processed_domains:
        with write_lock:
            rows_skipped += 1
        return

    nslookupA = row.get('nslookupA', '')
    ip_list = [ip.strip() for ip in str(nslookupA).split('|') if ip.strip()] if pd.notna(nslookupA) else []
    geo_data = {field: '' for field in geo_fields}

    for ip in ip_list:
        with cache_lock:
            if ip in ip_cache:
                geo_data = ip_cache[ip]
                break

        resp = fetch_ipinfo(ip)
        if not (resp and resp.status_code == 200):
            resp = fetch_ipinfo_with_proxies(ip)

        if resp and resp.status_code == 200:
            data = resp.json()
            geo_data = {field: data.get(field, '') for field in geo_fields}
            with cache_lock:
                ip_cache[ip] = geo_data
            break

        with cache_lock:
            ip_cache[ip] = geo_data

        time.sleep(0.5)

    for field in geo_fields:
        row[field] = geo_data[field]

    with write_lock:
        writer.writerow(row.to_dict())
        processed_domains.add(domain)
        rows_collected += 1

with open(output_file, mode='a', newline='', encoding='utf-8') as out_csv:
    writer = None
    file_empty = os.stat(output_file).st_size == 0

    for chunk_number, chunk in enumerate(pd.read_csv(input_file, chunksize=100, quotechar='"', on_bad_lines='skip')):
        if writer is None:
            writer = csv.DictWriter(out_csv, fieldnames=chunk.columns.tolist() + geo_fields)
            if file_empty:
                writer.writeheader()

        with ThreadPoolExecutor(max_workers=206) as executor:
            futures = [
                executor.submit(process_row, idx + 1 + chunk_number * 100, row, writer, processed_domains.copy())
                for idx, (_, row) in enumerate(chunk.iterrows())
            ]
            for future in as_completed(futures):
                pass

print(f"✅ Total collected now: {rows_collected}")
print(f"⏩ Total skipped (already processed): {rows_skipped}")
