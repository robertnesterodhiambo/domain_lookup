import pandas as pd
import os
import random

INPUT_FILE = 'db_excel_ns.csv'
OUTPUT_FILE = 'tes_accesibilty.csv'

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

# Load or initialize output
if os.path.exists(OUTPUT_FILE):
    df = pd.read_csv(OUTPUT_FILE)

    # Add missing input columns if needed
    for col in df_input.columns:
        if col not in df.columns:
            df[col] = df_input[col]
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

# Fill only unfilled rows
for index, row in df[df['_filled'] == False].iterrows():
    while True:
        values = {col: random.randint(*RANGES[col]) for col in COLUMNS}
        if sum(values.values()) > 100:
            break
    for col in COLUMNS:
        df.at[index, col] = values[col]

# Clean up helper column
df = df.drop(columns=['_filled'])

# Save
df.to_csv(OUTPUT_FILE, index=False)

# Report
print("✅ Accessibility data updated and saved.")
print(f"📊 Total rows: {total_rows}")
print(f"⏩ Skipped (already filled): {skipped_rows}")
print(f"🛠️  Processed (newly filled): {to_process}")
