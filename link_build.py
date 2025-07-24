import pandas as pd

tld_rdap = pd.read_csv('tld_rdap_mapping.csv')

chunk_size = 50000
output_file = 'data_rdap.csv'

first_chunk = True
chunk_num = 0

for chunk in pd.read_csv('lookup_with_counts_and_tld.csv', chunksize=chunk_size):
    chunk_num += 1

    merged_chunk = chunk.merge(tld_rdap, on='tld', how='left')

    merged_chunk['rdap_link'] = merged_chunk.apply(
        lambda x: f"{x['rdap'].rstrip('/')}/domain/{x['domain']}" if pd.notna(x['rdap']) else '',
        axis=1
    )

    merged_chunk.to_csv(output_file, mode='w' if first_chunk else 'a', index=False, header=first_chunk)

    print(f"Processed chunk {chunk_num} (rows {chunk_num * chunk_size - len(chunk) + 1} to {chunk_num * chunk_size})")

    first_chunk = False
