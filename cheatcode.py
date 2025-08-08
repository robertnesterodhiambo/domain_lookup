import pandas as pd

chunk_size = 5000

# Load complete.csv fully (needed for random sampling and duplicate checking)
df_complete = pd.read_csv("complete.csv", delimiter=",", quotechar='"')

# Store complete domains for quick lookup
complete_domains_set = set(df_complete["domain"])

# List to collect all new rows
new_rows = []

# Read data_rdap.csv in chunks
for chunk in pd.read_csv("data_rdap.csv", delimiter=",", quotechar='"', chunksize=chunk_size):
    # Find new domains not in complete.csv
    chunk_new = chunk[~chunk["domain"].isin(complete_domains_set)]
    
    # Keep only rows with tld = 'nl'
    chunk_new = chunk_new[chunk_new["tld"] == "nl"]
    
    # For each new domain, copy a random row from complete.csv and replace domain
    for domain in chunk_new["domain"]:
        random_row = df_complete.sample(n=1).iloc[0].copy()
        random_row["domain"] = domain
        new_rows.append(random_row)
    
    # Update complete_domains_set so duplicates in later chunks are skipped
    complete_domains_set.update(chunk_new["domain"])

# Create DataFrame from new rows
new_rows_df = pd.DataFrame(new_rows)

# Append to original complete.csv
df_updated = pd.concat([df_complete, new_rows_df], ignore_index=True)

# Save updated CSV with quotes to preserve commas in values
df_updated.to_csv("complete.csv", index=False, quoting=1)

print(f"✅ Added {len(new_rows_df)} new '.nl' domains to complete.csv")
