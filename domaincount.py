import pandas as pd
import os

# Define input and output paths
input_file = os.path.join("looku_file", "lookup.txt")
output_file = "lookup_with_counts_and_tld.csv"

# Read the domains from txt file into a DataFrame
with open(input_file, "r") as f:
    domains = [line.strip() for line in f if line.strip()]

df = pd.DataFrame(domains, columns=["domain"])

# Create 'count' column with total number of rows
df["count"] = len(df)

# Create 'tld' column by splitting domain by last dot
df["tld"] = df["domain"].apply(lambda x: x.split(".")[-1] if "." in x else "")

# Write to CSV
df.to_csv(output_file, index=False)

print(f"Processed {len(df)} domains. Output saved to {output_file}.")
