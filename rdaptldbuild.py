import pandas as pd
import os

# === File paths ===
lookup_file = "lookup_with_counts_and_tld.csv"
tld_rdap_file = "tld_rdap_mapping.csv"
output_file = "data_rdap.csv"

# === Settings ===
chunk_size = 100000  # adjust depending on memory
chunk_number = 0

# Load small reference file (tld_rdap)
tld_rdap = pd.read_csv(tld_rdap_file)

# Remove existing output file if any
if os.path.exists(output_file):
    os.remove(output_file)

# Process lookup file in chunks
for chunk in pd.read_csv(lookup_file, chunksize=chunk_size):
    chunk_number += 1

    # Merge chunk with tld_rdap
    merged = chunk.merge(tld_rdap, on="tld", how="left")

    # Append to output file
    merged.to_csv(output_file, mode="a", index=False, header=(chunk_number == 1))

    # Free memory
    del chunk, merged

    print(f"✅ Processed and saved chunk #{chunk_number}")

print(f"\n🎯 Done! All chunks processed and saved to {output_file}.")
