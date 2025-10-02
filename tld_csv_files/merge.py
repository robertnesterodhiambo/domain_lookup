import pandas as pd
import glob

# Path pattern for all your CSV files
files = glob.glob("*.csv")   # change to e.g. "/home/dragon/data/*.csv"

dfs = []
for f in files:
    try:
        df = pd.read_csv(f, quotechar='"', encoding="utf-8", dtype=str)  
        df["source_file"] = f  # optional: track which file it came from
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Skipping {f} due to error: {e}")

# Merge with all columns (outer join automatically handled by concat)
merged = pd.concat(dfs, axis=0, ignore_index=True, sort=True)

# Save merged CSV
merged.to_csv("merged_output.csv", index=False, encoding="utf-8", quoting=1)  # quoting=1 → QUOTE_ALL
print(f"✅ Merged {len(files)} CSV files → merged_output.csv")

