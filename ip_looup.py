import pandas as pd

lookup_path = 'looku_file/lookup.txt'

# Read only first 10 rows using nrows parameter
df = pd.read_csv(lookup_path, header=None, names=['data'], nrows=10, encoding='utf-8', engine='python')

print(df)
