import pandas as pd
import os
import random

INPUT_FILE = 'page_count.csv'
OUTPUT_FILE = 'database.csv'

# Define value ranges
RANGES = {
    'Violations': (0, 25),
    'Passes': (0, 25),
    'Incomplete': (0, 65),
    'Inapplicable': (0, 50)
}
COLUMNS = list(RANGES.keys())

# Load input
df_input = pd.read_csv(INPUT_FILE)

# If output exists, load it and merge with input
if os.path.exists(OUTPUT_FILE):
    df_existing = pd.read_csv(OUTPUT_FILE)

    # Merge to ensure all rows from input are present (assumes a unique column like "domain" exists)
    df = pd.merge(df_input, df_existing, how='left', on=df_input.columns.tolist(), suffixes=('', '_y'))

    # Add missing value columns
    for col in COLUMNS:
        if col not in df:
            df[col] = None
        elif f'{col}_y' in df:
            df[col] = df[f'{col}_y']
            df.drop(columns=[f'{col}_y'], inplace=True)
else:
    df = df_input.copy()
    for col in COLUMNS:
        df[col] = None

# Mark rows already filled
def is_filled(row):
    return all(pd.notna(row[col]) for col in COLUMNS)

df['_filled'] = df.apply(is_filled, axis=1)

# Count for reporting
total_rows = len(df)
skipped_rows = df['_filled'].sum()
to_process = total_rows - skipped_rows
print(f"Total rows: {total_rows}, Skipped (already filled): {skipped_rows}, To process: {to_process}")

# Fill only unfilled rows
for index, row in df[df['_filled'] == False].iterrows():
    while True:
        values = {col: random.randint(*RANGES[col]) for col in COLUMNS}
        if sum(values.values()) > 100:
            break
    for col in COLUMNS:
        df.at[index, col] = values[col]

# Clean up helper column
df.drop(columns=['_filled'], inplace=True)

# Save
df.to_csv(OUTPUT_FILE, index=False)

