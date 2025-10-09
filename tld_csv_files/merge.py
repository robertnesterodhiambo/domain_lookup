import pandas as pd
import glob

# Path pattern for all your CSV files
files = glob.glob("*.csv")   # change path if needed

dfs = []
for f in files:
    try:
        df = pd.read_csv(f, quotechar='"', encoding="utf-8", dtype=str)  
        df["source_file"] = f  # optional: track source file
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Skipping {f} due to error: {e}")

# Merge with all columns
merged = pd.concat(dfs, axis=0, ignore_index=True, sort=True)

# Save to Excel (better for long text, special characters)
output_file = "merged_output.xlsx"
merged.to_excel(output_file, index=False, engine="openpyxl")

print(f"✅ Merged {len(files)} CSV files → {output_file}")
