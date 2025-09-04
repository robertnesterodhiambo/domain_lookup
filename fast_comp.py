import pandas as pd
import random
import os

# File paths
INPUT_FILE = 'violations.csv'
OUTPUT_FILE = 'complete.csv'
DISTINCT_FILE = 'distinct_IP.csv'

# Geo fields only in output
GEO_FIELDS = ['city', 'region', 'country', 'loc', 'org', 'postal', 'timezone']

CHUNK_SIZE = 500

# Load supporting data
df_distinct = pd.read_csv(DISTINCT_FILE)

# Prepare IP lookup dict from distinct_IP.csv
ip_lookup = {
    ip.strip(): row for _, row in df_distinct.iterrows()
    for ip in str(row['nslookupA']).split('|')
}

# Deep random pool for unmatched rows
distinct_records = df_distinct[GEO_FIELDS].dropna().to_dict(orient="records")

skipped_count = 0
added_count = 0
chunk_number = 0

# Load existing output if available
if os.path.exists(OUTPUT_FILE):
    df_output = pd.read_csv(OUTPUT_FILE)
else:
    df_output = pd.DataFrame()

# Process input file in chunks
for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE):
    chunk_number += 1
    existing_domains = set(df_output['domain']) if not df_output.empty else set()
    df_new = chunk[~chunk['domain'].isin(existing_domains)].copy()

    skipped_count += chunk.shape[0] - df_new.shape[0]
    added_count += df_new.shape[0]

    if df_new.empty:
        print(f"Chunk {chunk_number}: skipped all {chunk.shape[0]} domains (already processed).")
        continue

    # Expand random pool sufficiently for chunk size
    extended_records = (distinct_records * ((df_new.shape[0] // len(distinct_records)) + 1))[:df_new.shape[0]]
    random.shuffle(extended_records)

    geo_data = []

    for _, row in df_new.iterrows():
        ns_ips = str(row['nslookupA']).split('|') if pd.notna(row['nslookupA']) else []
        ns_ips = [ip.strip() for ip in ns_ips]

        geo_assigned = None

        # Rule 1: If nslookupA exists in OUTPUT_FILE already
        if not df_output.empty:
            match_output = df_output.loc[df_output['nslookupA'] == row['nslookupA']]
            if not match_output.empty:
                geo_assigned = match_output.iloc[0][GEO_FIELDS].to_dict()

        # Rule 2: Match against distinct_IP.csv
        if geo_assigned is None:
            for ip in ns_ips:
                if ip in ip_lookup:
                    geo_assigned = ip_lookup[ip][GEO_FIELDS].to_dict()
                    break

        # Rule 3: Random assignment
        if geo_assigned is None:
            geo_assigned = extended_records.pop()

        geo_data.append(geo_assigned)

    # Convert geo_data into DataFrame
    geo_df = pd.DataFrame(geo_data, index=df_new.index)

    # Attach geo fields
    for col in GEO_FIELDS:
        df_new[col] = geo_df[col]

    # Save immediately (append mode)
    write_header = not os.path.exists(OUTPUT_FILE)
    df_new.to_csv(OUTPUT_FILE, mode='a', header=write_header, index=False)

    # Update in-memory output for future matching
    df_output = pd.concat([df_output, df_new], ignore_index=True)

    print(f"Chunk {chunk_number}: processed {chunk.shape[0]} rows, added {df_new.shape[0]}, skipped {chunk.shape[0] - df_new.shape[0]}. Saved to {OUTPUT_FILE}.")

print(f"\nFinal summary: Skipped {skipped_count} domains (already in {OUTPUT_FILE}).")
print(f"Added {added_count} new domains with geo fields (rules applied).")
