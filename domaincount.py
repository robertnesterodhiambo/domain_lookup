import pandas as pd
import os

# === File paths ===
input_file = os.path.join("looku_file", "lookup.txt")
output_file = "lookup_with_counts_and_tld.csv"

# === Settings ===
chunk_size = 100000  # adjust as needed (e.g. 50k, 200k)
total_lines = sum(1 for _ in open(input_file, 'r'))
chunk_number = 0
processed_total = 0

# Remove existing output file if any
if os.path.exists(output_file):
    os.remove(output_file)

# === Process file in chunks ===
with open(input_file, "r") as f:
    while True:
        # Read next chunk of lines
        lines = [line.strip() for _, line in zip(range(chunk_size), f) if line.strip()]
        if not lines:
            break  # end of file

        chunk_number += 1
        processed_total += len(lines)

        # Create DataFrame for this chunk
        df = pd.DataFrame(lines, columns=["domain"])
        df["count"] = total_lines
        df["tld"] = df["domain"].apply(lambda x: x.split(".")[-1] if "." in x else "")

        # Append to CSV (write header only for first chunk)
        df.to_csv(output_file, mode="a", index=False, header=(chunk_number == 1))

        # Free memory
        del df, lines

        print(f"✅ Processed chunk #{chunk_number} ({processed_total}/{total_lines} lines)")

print(f"\n🎯 Done! Total processed: {processed_total} lines. Output saved to {output_file}.")
