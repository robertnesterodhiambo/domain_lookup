import pandas as pd
import requests
import os

INPUT_CSV = 'data_rdap.csv'
OUTPUT_CSV = 'data_rdap_parsed.csv'

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

# Load input CSV
df = pd.read_csv(INPUT_CSV)

# Drop empty rdap_link
df = df[df['rdap_link'].notna()]
df = df[df['rdap_link'].str.strip() != '']

# Prioritize .nl TLD
df_sorted = pd.concat([df[df['tld'] == 'nl'], df[df['tld'] != 'nl']])

# Prepare output CSV columns
parsed_columns = [
    'ldhName', 'status', 'registration_date', 'last_changed_date', 'last_update_rdap_date',
    'registrant_name', 'registrant_email', 'admin_name', 'admin_email',
    'tech_name', 'tech_email', 'registrar_name', 'reseller_name',
    'nameservers', 'secureDNS_delegationSigned'
]
output_columns = list(df.columns) + parsed_columns

# Create output CSV if doesn't exist
if not os.path.exists(OUTPUT_CSV):
    pd.DataFrame(columns=output_columns).to_csv(OUTPUT_CSV, index=False)

# Load processed rdap_links to skip duplicates
processed = pd.read_csv(OUTPUT_CSV)
processed_links = set(processed['rdap_link'].tolist())

# Iterate and process
for _, row in df_sorted.iterrows():
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

        output_row = {col: row[col] for col in df.columns}
        output_row.update(parsed)

        pd.DataFrame([output_row]).to_csv(OUTPUT_CSV, mode='a', header=False, index=False)

        print(f"Saved data for: {link}")

    except Exception as e:
        print(f"Error processing {link}: {e}")
        continue

