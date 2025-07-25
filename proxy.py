#!/usr/bin/env python3

import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor
import threading

INPUT_CSV = 'data_rdap.csv'
OUTPUT_CSV = 'data_rdap_parsed.csv'
CHUNK_SIZE = 50000
LAST_CHUNK_FILE = 'last_chunk.txt'
THREADS = 2

write_lock = threading.Lock()

# Rate limit and proxy handling globals
RATE_LIMIT_THRESHOLD = 15
rate_limit_count = 0
use_proxy = False
rate_limit_lock = threading.Lock()

# Proxy credentials and settings
username = "geonode_DrXb2XNsHm-type-residential"
password = "f232262f-0f34-400c-a7a6-84d1ce423302"
GEONODE_DNS = "proxy.geonode.io:9000"
proxy_dict = {
    "http": f"http://{username}:{password}@{GEONODE_DNS}",
    "https": f"http://{username}:{password}@{GEONODE_DNS}"
}

def parse_rdap_json(rdap_data):
    parsed = {}

    parsed['ldhName'] = rdap_data.get('ldhName', '')
    parsed['status'] = ','.join(rdap_data.get('status', []))

    events = rdap_data.get('events', [])
    parsed['registration_date'] = ''
    parsed['last_changed_date'] = ''
    parsed['last_update_rdap_date'] = ''
    for event in events:
        action = event.get('eventAction')
        date = event.get('eventDate', '')
        if action == 'registration':
            parsed['registration_date'] = date
        elif action == 'last changed':
            parsed['last_changed_date'] = date
        elif action == 'last update of RDAP database':
            parsed['last_update_rdap_date'] = date

    parsed['registrant_name'] = ''
    parsed['registrant_email'] = ''
    parsed['admin_name'] = ''
    parsed['admin_email'] = ''
    parsed['tech_name'] = ''
    parsed['tech_email'] = ''
    parsed['registrar_name'] = ''
    parsed['registrar_addr'] = ''
    parsed['registrar_city'] = ''
    parsed['registrar_region'] = ''
    parsed['registrar_postalcode'] = ''
    parsed['registrar_country'] = ''
    parsed['reseller_name'] = ''

    for entity in rdap_data.get('entities', []):
        roles = entity.get('roles', [])
        vcard = entity.get('vcardArray', [None, []])[1]

        name = ''
        email = ''
        addr = ''
        city = ''
        region = ''
        postalcode = ''
        country = ''
        for item in vcard:
            if item[0] == 'fn':
                name = item[3]
            elif item[0] == 'email':
                email = item[3]
            elif item[0] == 'adr':
                adr_fields = item[3]
                if isinstance(adr_fields, list) and len(adr_fields) >= 7:
                    addr = adr_fields[2]
                    city = adr_fields[3]
                    region = adr_fields[4]
                    postalcode = adr_fields[5]
                    country = adr_fields[6]

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

    nameservers = [ns.get('ldhName', '') for ns in rdap_data.get('nameservers', [])]
    parsed['nameservers'] = ','.join(nameservers)

    parsed['secureDNS_delegationSigned'] = rdap_data.get('secureDNS', {}).get('delegationSigned', False)

    return parsed

def save_last_chunk(num):
    with open(LAST_CHUNK_FILE, 'w') as f:
        f.write(str(num))

def load_last_chunk():
    if os.path.exists(LAST_CHUNK_FILE):
        with open(LAST_CHUNK_FILE, 'r') as f:
            val = f.read().strip()
            if val.isdigit():
                return int(val)
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
    output_columns = list(df_head.columns) + parsed_columns
    pd.DataFrame(columns=output_columns).to_csv(OUTPUT_CSV, index=False)

processed = pd.read_csv(OUTPUT_CSV)
processed_links = set(processed['rdap_link'].dropna().astype(str).tolist())

start_chunk = load_last_chunk()

def process_row(row):
    global rate_limit_count, use_proxy

    link = row['rdap_link']
    if link in processed_links:
        print(f"Skipping already processed: {link}")
        return

    print(f"Processing: {link}")

    try:
        proxies = proxy_dict if use_proxy else None
        response = requests.get(link, timeout=10, proxies=proxies)

        if response.status_code == 429:
            with rate_limit_lock:
                rate_limit_count += 1
                print(f"Rate limit hit {rate_limit_count} times.")
                if rate_limit_count >= RATE_LIMIT_THRESHOLD:
                    if not use_proxy:
                        use_proxy = True
                        print("Switched to proxy due to repeated rate limits.")
            return

        elif response.status_code != 200:
            print(f"Failed to fetch {link} with status {response.status_code}, skipping.")
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
        print(f"Error processing {link}: {e}")

chunk_number = 0
for chunk in pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE):
    chunk_number += 1

    if chunk_number <= start_chunk:
        print(f"Skipping chunk #{chunk_number} (already processed)")
        continue

    print(f"\nProcessing chunk #{chunk_number} with {len(chunk)} rows")
    print(chunk.head())

    chunk = chunk[chunk['rdap_link'].notna()].copy()
    chunk['rdap_link'] = chunk['rdap_link'].astype(str)
    chunk = chunk[chunk['rdap_link'].str.strip() != '']

    chunk_sorted = pd.concat([chunk[chunk['tld'] == 'nl'], chunk[chunk['tld'] != 'nl']])

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(process_row, [row for _, row in chunk_sorted.iterrows()])

    save_last_chunk(chunk_number)
