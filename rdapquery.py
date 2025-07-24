import pandas as pd
import requests
import os

INPUT_CSV = 'data_rdap.csv'
OUTPUT_CSV = 'data_rdap_parsed.csv'
CHUNK_SIZE = 50000
LAST_CHUNK_FILE = 'last_chunk.txt'  # file to store last processed chunk number

def parse_rdap_json(rdap_data):
    parsed = {}

    parsed['ldhName'] = rdap_data.get('ldhName', '')
    parsed['status'] = ','.join(rdap_data.get('status', []))

    # Events
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

    # Entities
    parsed['registrant_name'] = ''
    parsed['registrant_email'] = ''
    parsed['admin_name'] = ''
    parsed['admin_email'] = ''
    parsed['tech_name'] = ''
    parsed['tech_email'] = ''
    parsed['registrar_name'] = ''
    parsed['reseller_name'] = ''

    for entity in rdap_data.get('entities', []):
        roles = entity.get('roles', [])
        vcard = entity.get('vcardArray', [None, []])[1]

        name = ''
        email = ''
        for item in vcard:
            if item[0] == 'fn':
                name = item[3]
            elif item[0] == 'email':
                email = item[3]

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
        elif 'reseller' in roles:
            parsed['reseller_name'] = name

    # Nameservers
    nameservers = [ns.get('ldhName', '') for ns in rdap_data.get('nameservers', [])]
    parsed['nameservers'] = ','.join(nameservers)

    # SecureDNS
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
    'tech_name', 'tech_email', 'registrar_name', 'reseller_name',
    'nameservers', 'secureDNS_delegationSigned'
]

if not os.path.exists(OUTPUT_CSV):
    df_head = pd.read_csv(INPUT_CSV, nrows=1)
    output_columns = list(df_head.columns) + parsed_columns
    pd.DataFrame(columns=output_columns).to_csv(OUTPUT_CSV, index=False)

processed = pd.read_csv(OUTPUT_CSV)
processed_links = set(processed['rdap_link'].tolist())

start_chunk = load_last_chunk()

chunk_number = 0
for chunk in pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE):
    chunk_number += 1

    if chunk_number <= start_chunk:
        print(f"Skipping chunk #{chunk_number} (already processed)")
        continue

    print(f"\nProcessing chunk #{chunk_number} with {len(chunk)} rows")
    print(chunk.head())  # show first 5 rows for debug

    chunk = chunk[chunk['rdap_link'].notna()].copy()
    chunk['rdap_link'] = chunk['rdap_link'].astype(str)
    chunk = chunk[chunk['rdap_link'].str.strip() != '']

    chunk_sorted = pd.concat([chunk[chunk['tld'] == 'nl'], chunk[chunk['tld'] != 'nl']])

    for _, row in chunk_sorted.iterrows():
        link = row['rdap_link']
        if link in processed_links:
            print(f"Skipping already processed: {link}")
            continue

        print(f"Processing: {link}")
        try:
            response = requests.get(link, timeout=10)
            if response.status_code == 429:
                print(f"Rate limit exceeded for {link}, skipping for now.")
                continue
            elif response.status_code != 200:
                print(f"Failed to fetch {link} with status {response.status_code}, skipping.")
                continue

            rdap_json = response.json()
            parsed = parse_rdap_json(rdap_json)

            output_row = {col: row[col] for col in row.index}
            output_row.update(parsed)

            pd.DataFrame([output_row]).to_csv(OUTPUT_CSV, mode='a', header=False, index=False)

            processed_links.add(link)

            print(f"Saved data for: {link}")

        except Exception as e:
            print(f"Error processing {link}: {e}")
            continue

    # Save progress after chunk processed
    save_last_chunk(chunk_number)
