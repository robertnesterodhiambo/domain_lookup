import pandas as pd
import random

# File paths (do not change)
INPUT_FILE = 'data_rdap_parsed.csv'
OUTPUT_FILE = 'domain_count.csv'

# Load both files
df_input = pd.read_csv(INPUT_FILE)
df_output = pd.read_csv(OUTPUT_FILE)

# Domains already in output
existing_domains = set(df_output['domain'])

# Filter input: keep only rows not in output
df_new = df_input[~df_input['domain'].isin(existing_domains)].copy()

# Count skipped and to-be-added
skipped_count = df_input.shape[0] - df_new.shape[0]
added_count = df_new.shape[0]

# If there are new rows to add
if added_count > 0:
    # Get random subdomain_count values from existing output
    subdomain_choices = df_output['subdomain_count'].dropna().tolist()
    if not subdomain_choices:
        raise ValueError("No subdomain_count values found in output file to sample from.")

    # Assign random subdomain_count to each new row
    df_new['subdomain_count'] = [random.choice(subdomain_choices) for _ in range(added_count)]

    # Append new rows to output file
    df_final = pd.concat([df_output, df_new], ignore_index=True)

    # Save back to OUTPUT_FILE
    df_final.to_csv(OUTPUT_FILE, index=False)

print(f"Skipped {skipped_count} domains (already in output).")
print(f"Added {added_count} new domains with random subdomain_count.")
