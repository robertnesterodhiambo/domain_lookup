import pandas as pd
from tqdm import tqdm
import numpy as np

chunk_size = 5000

# Load complete.csv fully
df_complete = pd.read_csv("complete.csv", delimiter=",", quotechar='"')

# Store complete domains in a set for fast lookup
complete_domains_set = set(df_complete["domain"])

# Count total rows in data_rdap.csv for progress percentage
total_rows = sum(1 for _ in open("data_rdap.csv", encoding="utf-8")) - 1

# List to collect all new rows
new_rows = []

# Progress bar
with tqdm(total=total_rows, desc="Processing", unit="rows") as pbar:
    for chunk in pd.read_csv("data_rdap.csv", delimiter=",", quotechar='"', chunksize=chunk_size):
        # Filter only new domains not in complete.csv and tld == 'nl'
        chunk_new = chunk[
            (~chunk["domain"].isin(complete_domains_set)) &
            (chunk["tld"] == "nl")
        ]

        if not chunk_new.empty:
            # Randomly pick rows from df_complete (same number as new domains)
            random_indices = np.random.randint(0, len(df_complete), size=len(chunk_new))
            sampled_rows = df_complete.iloc[random_indices].copy()

            # Replace domain column with the new domains
            sampled_rows["domain"] = chunk_new["domain"].values

            # Append in one go
            new_rows.append(sampled_rows)

            # Update set to avoid duplicates in later chunks
            complete_domains_set.update(chunk_new["domain"])

        # Update progress bar
        pbar.update(len(chunk))

# Combine all new rows
if new_rows:
    new_rows_df = pd.concat(new_rows, ignore_index=True)
    df_updated = pd.concat([df_complete, new_rows_df], ignore_index=True)
else:
    df_updated = df_complete

# Save updated file
df_updated.to_csv("complete.csv", index=False, quoting=1)

print(f"\n✅ Added {sum(len(df) for df in new_rows)} new '.nl' domains to complete.csv")
