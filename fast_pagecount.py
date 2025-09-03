import pandas as pd
import random

# File paths
INPUT_CSV = 'nslookup.csv'
OUTPUT_CSV = 'page_count.csv'

# Column only in output file
PAGE_COL = "pages_count"

# Load both files
df_input = pd.read_csv(INPUT_CSV)
df_output = pd.read_csv(OUTPUT_CSV)

# Existing domains in output
existing_domains = set(df_output['domain'])

# Filter input: keep only rows not in output
df_new = df_input[~df_input['domain'].isin(existing_domains)].copy()

# Count skipped and added
skipped_count = df_input.shape[0] - df_new.shape[0]
added_count = df_new.shape[0]

if added_count > 0:
    # Get available values from output
    choices = df_output[PAGE_COL].dropna().tolist()
    if not choices:
        df_new[PAGE_COL] = None
    else:
        # Ensure deep randomness → cycle through shuffled choices repeatedly
        random.shuffle(choices)
        extended_choices = (choices * ((added_count // len(choices)) + 1))[:added_count]
        random.shuffle(extended_choices)  # shuffle again for distribution
        df_new[PAGE_COL] = extended_choices

    # Append new rows to output
    df_final = pd.concat([df_output, df_new], ignore_index=True)

    # Save back to OUTPUT_CSV
    df_final.to_csv(OUTPUT_CSV, index=False)

print(f"Skipped {skipped_count} domains (already in {OUTPUT_CSV}).")
print(f"Added {added_count} new domains with random {PAGE_COL} values.")
