import pandas as pd

# Load the CSV files
tld_rdap = pd.read_csv('tld_rdap_mapping.csv')
lookup = pd.read_csv('lookup_with_counts_and_tld.csv')

# Merge on 'tld'
merged = lookup.merge(tld_rdap, on='tld', how='left')

# Build the full RDAP query link by adding '/domain/' + domain
# Assuming your lookup file has a column named 'domain' with domain names like '002.nl'
merged['rdap_link'] = merged['rdap'].str.rstrip('/') + '/domain/' + merged['domain'].astype(str)

# Save the result
merged.to_csv('data_rdap.csv', index=False)
