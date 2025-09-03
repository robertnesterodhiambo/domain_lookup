import pandas as pd
import random

# File paths
INPUT_FILE = 'violations.csv'
OUTPUT_FILE = 'complete.csv'
DISTINCT_FILE = 'distinct_IP.csv'

# Geo fields only in output
GEO_FIELDS = ['city', 'region', 'country', 'loc', 'org', 'postal', 'timezone']

# Load data
df_input = pd.read_csv(INPUT_FILE)
df_output = pd.read_csv(OUTPUT_FILE)
df_distinct = pd.read_csv(DISTINCT_FILE)

# Prepare IP lookup dict from distinct_IP.csv
ip_lookup = {ip.strip(): row for _, row in df_distinct.iterrows() for ip in str(row['nslookupA']).split('|')}

# Existing domains in output
existing_domains = set(df_output['domain'])

# Filter input: keep only rows not already in output
df_new = df_input[~df_input['domain'].isin(existing_domains)].copy()

# Count skipped and added
skipped_count = df_input.shape[0] - df_new.shape[0]
added_count = df_new.shape[0]

if added_count > 0:
    # Deep random pool from distinct_IP.csv for unmatched rows
    distinct_records = df_distinct[GEO_FIELDS].dropna().to_dict(orient="records")
    random.shuffle(distinct_records)
    extended_records = (distinct_records * ((added_count // len(distinct_records)) + 1))[:added_count]
    random.shuffle(extended_records)
    
    geo_data = []

    for idx, row in df_new.iterrows():
        ns_ips = str(row['nslookupA']).split('|') if pd.notna(row['nslookupA']) else []
        ns_ips = [ip.strip() for ip in ns_ips]

        geo_assigned = None

        # Rule 1: If nslookupA exists in OUTPUT_FILE already
        match_output = df_output.loc[df_output['nslookupA'] == row['nslookupA']]
        if not match_output.empty:
            geo_assigned = match_output.iloc[0][GEO_FIELDS].to_dict()

        # Rule 2: Match against distinct_IP.csv
        if geo_assigned is None:
            for ip in ns_ips:
                if ip in ip_lookup:
                    geo_assigned = ip_lookup[ip][GEO_FIELDS].to_dict()
                    break

        # Rule 3: Random assignment from distinct records
        if geo_assigned is None:
            geo_assigned = extended_records.pop()

        geo_data.append(geo_assigned)

    # Convert geo_data list of dicts into columns
    geo_df = pd.DataFrame(geo_data, index=df_new.index)

    # Attach geo fields
    for col in GEO_FIELDS:
        df_new[col] = geo_df[col]

    # Append to output
    df_final = pd.concat([df_output, df_new], ignore_index=True)

    # Save final
    df_final.to_csv(OUTPUT_FILE, index=False)

print(f"Skipped {skipped_count} domains (already in {OUTPUT_FILE}).")
print(f"Added {added_count} new domains with geo fields (rules applied).")
