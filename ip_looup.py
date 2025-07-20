import whois
import pandas as pd
import json
import os
from time import sleep

lookup_path = 'looku_file/lookup.txt'
output_path = 'whois_results.xlsx'
batch_size = 100

# Define the fields you want to extract and store
fields = [
    "domain_name", "registrar", "registrar_url", "reseller", "whois_server", "referral_url",
    "updated_date", "creation_date", "expiration_date", "name_servers", "status", "emails",
    "dnssec", "name", "org", "address", "city", "state", "registrant_postal_code", "country"
]

# Create the Excel file if not exists
if not os.path.exists(output_path):
    df = pd.DataFrame(columns=['domain'] + fields)
    df.to_excel(output_path, index=False)

# Read already processed domains to avoid duplicates
existing_df = pd.read_excel(output_path)
processed_domains = set(existing_df['domain'].dropna().astype(str).str.lower().tolist())

# Process the file in chunks
with open(lookup_path, 'r') as f:
    while True:
        batch = []
        try:
            for _ in range(batch_size):
                line = f.readline()
                if not line:
                    break
                domain = line.strip().lower()
                if domain and domain not in processed_domains:
                    batch.append(domain)
        except Exception as e:
            print(f"Error reading lines: {e}")
            break

        if not batch:
            print("No new domains to process.")
            break

        # Process each domain
        for domain in batch:
            print(f"\nProcessing domain: {domain}")
            try:
                w = whois.whois(domain)
                raw_json = json.dumps(w.__dict__, default=str, indent=2)
                print(f"=== RAW WHOIS OBJECT for {domain} ===\n{raw_json}\n")
                record = {'domain': domain}
                for field in fields:
                    record[field] = w.__dict__.get(field, None)
            except Exception as e:
                print(f"WHOIS lookup failed for {domain}: {e}")
                record = {'domain': domain}
                for field in fields:
                    record[field] = None

            # Save the result immediately
            try:
                new_row = pd.DataFrame([record])
                with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    existing_data = pd.read_excel(output_path)
                    combined_df = pd.concat([existing_data, new_row], ignore_index=True)
                    combined_df.to_excel(writer, index=False)
                processed_domains.add(domain)
                sleep(1)  # Optional: prevent rate limits
            except Exception as e:
                print(f"Error saving data for {domain}: {e}")
