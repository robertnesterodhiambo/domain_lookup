import pandas as pd

# Read CSV
df = pd.read_csv("data_rdap.csv")

# Filter rows where column 'tld' equals 'tld'
filtered_df = df[df["tld"] == "nl"]

# Save or display result
print(filtered_df)
# filtered_df.to_csv("filtered_data_rdap.csv", index=False)
