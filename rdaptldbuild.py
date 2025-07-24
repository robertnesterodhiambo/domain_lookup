import pandas as pd

# Load both CSV files
tld_rdap = pd.read_csv('tld_rdap_mapping.csv')
lookup = pd.read_csv('lookup_with_counts_and_tld.csv')

# Merge on 'tld' column
merged = lookup.merge(tld_rdap, on='tld', how='left')

# Save the merged result
merged.to_csv('data_rdap.csv', index=False)
