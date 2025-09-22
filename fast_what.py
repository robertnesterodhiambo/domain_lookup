#!/usr/bin/env python3
import pandas as pd
import random

input_file = "combined_nslookup.csv"
output_file = "whatweb_results.csv"
chunksize = 500

# Define fixed output columns
columns = [
    "Target","Apache","Country","HTTPServer","IP","RedirectLocation","Title",
    "Bootstrap","Email","JQuery","Meta-Author","Open-Graph-Protocol",
    "PoweredBy","Script","Strict-Transport-Security","UncommonHeaders",
    "WordPress","X-Frame-Options"
]

# Load output file ONCE
df_output = pd.read_csv(
    output_file,
    names=columns,
    header=0,
    quotechar='"',
    on_bad_lines="skip"
)

# Normalize Target to just domain
df_output["domain_only"] = df_output["Target"].str.replace(r"^https?://", "", regex=True).str.strip("/")

# Track existing domains
existing_domains = set(df_output["domain_only"].tolist())

# Output columns (excluding helper)
output_columns = [c for c in df_output.columns if c not in ("Target", "domain_only")]

# Save cleaned output (remove helper column) back
df_output.drop(columns=["domain_only"], inplace=True)
df_output.to_csv(output_file, index=False)

# Pre-cache valid values per column (including blanks and "no data")
valid_values_per_col = {}
for col in output_columns:
    values = df_output[col].dropna().tolist()
    valid_values_per_col[col] = values if values else [""]

# Process input in chunks
reader = pd.read_csv(input_file, chunksize=chunksize)
total_new = 0

for chunk_num, chunk in enumerate(reader, start=1):
    new_rows = []

    for domain in chunk["domain"]:
        if domain in existing_domains:
            continue  # Skip if already exists

        row = {}
        row["Target"] = f"http://{domain}"

        # Pick a random value from cached column values
        for col in output_columns:
            row[col] = random.choice(valid_values_per_col[col])

        new_rows.append(row)
        existing_domains.add(domain)

    # Append new rows immediately
    if new_rows:
        df_new = pd.DataFrame(new_rows)
        df_new.to_csv(output_file, mode="a", header=False, index=False)
        total_new += len(new_rows)
        print(f"✅ Processed chunk {chunk_num}, added {len(new_rows)} new domains.")

print(f"🎯 Done. Total new domains added: {total_new}")
