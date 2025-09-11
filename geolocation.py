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
complete_data_map = {}  # domain -> geo_fields
rows_collected = 0
rows_skipped = 0
rows_filled_from_complete = 0

# === Load complete.csv into map ===
if os.path.exists(output_file):
    try:
        existing_df = pd.read_csv(output_file, usecols=['domain'] + geo_fields)
        existing_df = existing_df.dropna(subset=['domain'])

        for _, row in existing_df.iterrows():
            domain = str(row['domain']).strip()
            complete_data_map[domain] = {field: row.get(field, '') for field in geo_fields}
            processed_domains.add(domain)

        rows_collected = len(complete_data_map)
        print(f"✅ Already collected: {rows_collected} rows from complete.csv")
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

def process_row(row_number, row, writer, fieldnames):
    global rows_collected, rows_skipped, rows_filled_from_complete

    domain = str(row.get('domain', '')).strip()
    print(f"📄 Processing row {row_number} - domain: {domain}")

    if not domain:
        with write_lock:
            rows_skipped += 1
        return

    # === Skip if already processed ===
    if domain in processed_domains:
        with write_lock:
            rows_skipped += 1
        return

    # === If in complete_data_map, reuse geo data ===
    if domain in complete_data_map:
        with write_lock:
            geo_data = complete_data_map[domain]
            for field in geo_fields:
                row[field] = geo_data[field]
            writer.writerow({col: row.get(col, '') for col in fieldnames})
            processed_domains.add(domain)
            rows_filled_from_complete += 1
        return

    # === Else fetch from IP ===
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
        writer.writerow({col: row.get(col, '') for col in fieldnames})
        processed_domains.add(domain)
        rows_collected += 1


# === Process input file ===
fieldnames = None
any_rows_written = False

for chunk_number, chunk in enumerate(pd.read_csv(input_file, chunksize=100, quotechar='"', on_bad_lines='skip')):
    print(f"\n📦 Importing chunk {chunk_number + 1}")

    if fieldnames is None:
        fieldnames = chunk.columns.tolist() + geo_fields

    with open(output_file, mode='a', newline='', encoding='utf-8') as out_csv:
        file_empty = os.stat(output_file).st_size == 0
        writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
        if file_empty:
            writer.writeheader()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(process_row, idx + 1 + chunk_number * 100, row, writer, fieldnames)
                for idx, (_, row) in enumerate(chunk.iterrows())
            ]
            for future in as_completed(futures):
                pass

        if not file_empty:
            any_rows_written = True

if not any_rows_written:
    print("ℹ️ No new domains were processed — nothing was added to complete.csv")

# === Final summary ===
print(f"\n✅ Total fetched from IP: {rows_collected}")
print(f"⏩ Total skipped (already processed or no domain): {rows_skipped}")
print(f"♻️ Reused from complete.csv: {rows_filled_from_complete}")
