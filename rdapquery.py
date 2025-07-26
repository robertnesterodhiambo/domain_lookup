#!/usr/bin/env python3

import pandas as pd
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor
import threading

INPUT_CSV = 'data_rdap.csv'
OUTPUT_CSV = 'data_rdap_parsed.csv'
CHUNK_SIZE = 50000
LAST_CHUNK_FILE = 'last_chunk.txt'
THREADS = 305

write_lock = threading.Lock()
proxy_lock = threading.Lock()

# Authenticated GeoNode proxy credentials
username = "geonode_DrXb2XNsHm-type-residential"
password = "f232262f-0f34-400c-a7a6-84d1ce423302"
GEONODE_DNS = "proxy.geonode.io:9000"

# List of proxies
PROXIES = [
    {
        "http": f"http://{username}:{password}@{GEONODE_DNS}",
        "https": f"http://{username}:{password}@{GEONODE_DNS}"
    }
]

proxy_index = [0]

def get_next_proxy():
    with proxy_lock:
        idx = proxy_index[0]
        proxy_index[0] = (idx + 1) % len(PROXIES)
        return PROXIES[idx]

def parse_rdap_json(rdap_data):
    parsed = {
        'ldhName': rdap_data.get('ldhName', ''),
        'status': ','.join(rdap_data.get('status', [])),
        'registration_date': '',
        'last_changed_date': '',
        'last_update_rdap_date': '',
        'registrant_name': '', 'registrant_email': '',
        'admin_name': '', 'admin_email': '',
        'tech_name': '', 'tech_email': '',
        'registrar_name': '', 'registrar_addr': '', 'registrar_city': '',
        'registrar_region': '', 'registrar_postalcode': '', 'registrar_country': '',
        'reseller_name': '',
        'nameservers': ','.join(ns.get('ldhName', '') for ns in rdap_data.get('nameservers', [])),
        'secureDNS_delegationSigned': rdap_data.get('secureDNS', {}).get('delegationSigned', False)
    }

    for event in rdap_data.get('events', []):
        action, date = event.get('eventAction'), event.get('eventDate', '')
        if action == 'registration':
            parsed['registration_date'] = date
        elif action == 'last changed':
            parsed['last_changed_date'] = date
        elif action == 'last update of RDAP database':
            parsed['last_update_rdap_date'] = date

    for entity in rdap_data.get('entities', []):
        roles = entity.get('roles', [])
        vcard = entity.get('vcardArray', [None, []])[1]

        name = email = addr = city = region = postalcode = country = ''
        for item in vcard:
            if item[0] == 'fn':
                name = item[3]
            elif item[0] == 'email':
                email = item[3]
            elif item[0] == 'adr':
                adr = item[3]
                if isinstance(adr, list) and len(adr) >= 7:
                    addr, city, region, postalcode, country = adr[2:7]

        if 'registrant' in roles:
            parsed['registrant_name'] = name
            parsed['registrant_email'] = email
        elif 'administrative' in roles:
            parsed['admin_name'] = name
            parsed['admin_email'] = email
        elif 'technical' in roles:
            parsed['tech_name'] = name
            parsed['tech_email'] = email
        elif 'registrar' in roles:
            parsed['registrar_name'] = name
            parsed['registrar_addr'] = addr
            parsed['registrar_city'] = city
            parsed['registrar_region'] = region
            parsed['registrar_postalcode'] = postalcode
            parsed['registrar_country'] = country
        elif 'reseller' in roles:
            parsed['reseller_name'] = name

    return parsed

def save_last_chunk(num):
    with open(LAST_CHUNK_FILE, 'w') as f:
        f.write(str(num))

def load_last_chunk():
    if os.path.exists(LAST_CHUNK_FILE):
        with open(LAST_CHUNK_FILE, 'r') as f:
            val = f.read().strip()
            return int(val) if val.isdigit() else 0
    return 0

parsed_columns = [
    'ldhName', 'status', 'registration_date', 'last_changed_date', 'last_update_rdap_date',
    'registrant_name', 'registrant_email', 'admin_name', 'admin_email',
    'tech_name', 'tech_email', 'registrar_name',
    'registrar_addr', 'registrar_city', 'registrar_region', 'registrar_postalcode', 'registrar_country',
    'reseller_name', 'nameservers', 'secureDNS_delegationSigned'
]

if not os.path.exists(OUTPUT_CSV):
    df_head = pd.read_csv(INPUT_CSV, nrows=1)
    pd.DataFrame(columns=list(df_head.columns) + parsed_columns).to_csv(OUTPUT_CSV, index=False)

processed = pd.read_csv(OUTPUT_CSV)
processed_links = set(processed['rdap_link'].dropna().astype(str).tolist())

start_chunk = load_last_chunk()

def process_row(row):
    link = row['rdap_link']
    if link in processed_links:
        print(f"Skipping already processed: {link}")
        return

    proxy = get_next_proxy()
    print(f"Fetching {link} using proxy {proxy['http']}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Accept": "application/rdap+json, application/json, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(link, headers=headers, timeout=15, proxies=proxy)
        if response.status_code != 200:
            print(f"Non-200 status for {link}: {response.status_code}")
            return

        rdap_json = response.json()
        parsed = parse_rdap_json(rdap_json)

        output_row = {col: row[col] for col in row.index}
        output_row.update(parsed)

        with write_lock:
            pd.DataFrame([output_row]).to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
            processed_links.add(link)
            print(f"Saved data for: {link}")

    except Exception as e:
        print(f"Request failed for {link} with proxy {proxy['http']}: {e}")

chunk_number = 0
for chunk in pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE):
    chunk_number += 1
    if chunk_number <= start_chunk:
        print(f"Skipping chunk #{chunk_number} (already processed)")
        continue

    print(f"\nProcessing chunk #{chunk_number} with {len(chunk)} rows")

    chunk = chunk[chunk['rdap_link'].notna()].copy()
    chunk['rdap_link'] = chunk['rdap_link'].astype(str).str.strip()
    chunk = chunk[chunk['rdap_link'] != '']

    # Process .nl first
    nl_chunk = chunk[chunk['tld'] == 'nl']
    if nl_chunk.empty:
        print(f"No .nl domains in chunk #{chunk_number}, skipping entire chunk.")
        continue

    chunk_sorted = pd.concat([nl_chunk, chunk[chunk['tld'] != 'nl']])

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(process_row, [row for _, row in chunk_sorted.iterrows()])

    save_last_chunk(chunk_number)
