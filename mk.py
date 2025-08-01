import pandas as pd
import numpy as np
import os


# File paths
violations_path = "/root/domain/violations.csv"
page_count_path = "/root/domain/page_count.csv"

# Configuration
chunk_size = 100
copy_cols = ['violations', 'passes', 'incomplete']
domain_col = 'domain'

# === Step 1: Prepare initial violations.csv (if exists) ===
if os.path.exists(violations_path):
    violations_df = pd.read_csv(violations_path)
    violations_df[domain_col] = violations_df[domain_col].astype(str).str.strip().str.lower()
    existing_domains = set(violations_df[domain_col])
else:
    violations_df = pd.DataFrame()
    existing_domains = set()
    # Create empty file with placeholder header later if needed

# === Step 2: Process page_count.csv in chunks ===
chunk_number = 0
header_written = os.path.exists(violations_path)

for chunk in pd.read_csv(page_count_path, chunksize=chunk_size):
    chunk_number += 1
    chunk = chunk.copy()

    # Normalize domain column
    chunk[domain_col] = chunk[domain_col].astype(str).str.strip().str.lower()

    # Filter out duplicates
    original_len = len(chunk)
    chunk = chunk[~chunk[domain_col].isin(existing_domains)]
    new_len = len(chunk)

    print(f"[Chunk {chunk_number} - page_count.csv] Imported: {original_len}, New domains added: {new_len}")

    if chunk.empty:
        continue

    # Inject the 3 columns
    if not violations_df.empty:
        for i in chunk.index:
            sample = violations_df[copy_cols].sample(1).iloc[0]
            if any(
                str(val).lower() == 'site unreachable' or
                not str(val).strip().replace('.', '', 1).isdigit()
                for val in sample.values
            ):
                for col in copy_cols:
                    chunk.loc[i, col] = 'site unreachable'
            else:
                for col, val in zip(copy_cols, sample.values):
                    chunk.loc[i, col] = val
    else:
        # violations.csv is empty, so fill default values
        for col in copy_cols:
            chunk[col] = 'site unreachable'

    # === Step 3: Append chunk to violations.csv ===
    chunk.to_csv(violations_path, mode='a', header=not header_written, index=False)
    header_written = True

    # Update in-memory tracker
    existing_domains.update(chunk[domain_col])

print(f"\n✅ All chunks processed and written. Final file is at: {violations_path}")

