import pandas as pd
import random

# File paths
INPUT_FILE = 'domain_count.csv'
OUTPUT_FILE = 'nslookup.csv'

# Columns only in nslookup file
NSLOOKUP_COLS = [
    "nslookupA","nslookupAAAA","nslookupCNAME","nslookupMX","nslookupNS",
    "nslookupTXT","nslookupSRV","nslookupSOA","nslookupPTR","nslookuptxt_dmarc"
]

# Load both files
df_input = pd.read_csv(INPUT_FILE)
df_output = pd.read_csv(OUTPUT_FILE)

# Existing domains in output
existing_domains = set(df_output['domain'])

# Filter input: keep only rows not in output
df_new = df_input[~df_input['domain'].isin(existing_domains)].copy()

# Count skipped and added
skipped_count = df_input.shape[0] - df_new.shape[0]
added_count = df_new.shape[0]

if added_count > 0:
    # For each nslookup column, pick random values from that column in output
    for col in NSLOOKUP_COLS:
        choices = df_output[col].dropna().tolist()
        if not choices:  # if no values exist yet
            df_new[col] = None
        else:
            df_new[col] = [random.choice(choices) for _ in range(added_count)]

    # Append new rows to output
    df_final = pd.concat([df_output, df_new], ignore_index=True)

    # Save back to OUTPUT_FILE
    df_final.to_csv(OUTPUT_FILE, index=False)

print(f"Skipped {skipped_count} domains (already in {OUTPUT_FILE}).")
print(f"Added {added_count} new domains with random values per column.")
