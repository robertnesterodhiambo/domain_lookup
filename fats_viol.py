import pandas as pd
import random

# File paths
INPUT_CSV = 'page_count.csv'
OUTPUT_CSV = 'violations.csv'

# Columns only in output file
VIOLATION_COLS = ["violations", "passes", "incomplete"]

# Load both files
df_input = pd.read_csv(INPUT_CSV)
df_output = pd.read_csv(OUTPUT_CSV)

# Existing domains in output
existing_domains = set(df_output['domain'])

# Filter input: keep only rows not in output
df_new = df_input[~df_input['domain'].isin(existing_domains)].copy()

# Count skipped and added
skipped_count = df_input.shape[0] - df_new.shape[0]
added_count = df_new.shape[0]

if added_count > 0:
    # Get deep-randomized lists for each column
    choices_dict = {}
    for col in VIOLATION_COLS:
        choices = df_output[col].dropna().tolist()
        if not choices:
            choices_dict[col] = [None] * added_count
        else:
            random.shuffle(choices)
            extended = (choices * ((added_count // len(choices)) + 1))[:added_count]
            random.shuffle(extended)
            choices_dict[col] = extended

    # Assign values row by row with "site unreachable" rule
    violations_list = []
    passes_list = []
    incomplete_list = []

    for i in range(added_count):
        row_vals = {col: choices_dict[col][i] for col in VIOLATION_COLS}
        
        if "site unreachable" in row_vals.values():
            # If any column picked "site unreachable" → force all 3
            violations_list.append("site unreachable")
            passes_list.append("site unreachable")
            incomplete_list.append("site unreachable")
        else:
            violations_list.append(row_vals["violations"])
            passes_list.append(row_vals["passes"])
            incomplete_list.append(row_vals["incomplete"])

    df_new["violations"] = violations_list
    df_new["passes"] = passes_list
    df_new["incomplete"] = incomplete_list

    # Append new rows to output
    df_final = pd.concat([df_output, df_new], ignore_index=True)

    # Save back to OUTPUT_CSV
    df_final.to_csv(OUTPUT_CSV, index=False)

print(f"Skipped {skipped_count} domains (already in {OUTPUT_CSV}).")
print(f"Added {added_count} new domains with random values for {', '.join(VIOLATION_COLS)} (site unreachable rule applied).")
