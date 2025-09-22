#!/usr/bin/env python3
"""
fill_missing_shodan.py

- Reads input: combined_nslookup.csv  (columns: domain, nslookup)
- Reads output: shodan_results.csv     (columns: domain, nslookup, ip, cpes, hostnames, ports, tags, vulns)

Adds only missing domains from input into output:
- domain + nslookup from input
- other fields (ip, cpes, hostnames, ports, tags, vulns) randomly sampled from the same column in existing output
- avoids duplicates (domains already present are skipped)

Processes input in chunks of 500 and saves results immediately (append mode).
"""

import pandas as pd
import numpy as np
import os
import argparse

EXPECTED_COLS = ["domain","nslookup","ip","cpes","hostnames","ports","tags","vulns"]

def main(infile, outfile, seed=None):
    if seed is not None:
        np.random.seed(seed)

    # Check input file
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Input file not found: {infile}")

    # Load existing output (if any)
    if os.path.exists(outfile):
        out_df = pd.read_csv(outfile, dtype=str).fillna("")
    else:
        print(f"⚠️ Output file '{outfile}' not found — will create a new one.")
        out_df = pd.DataFrame(columns=EXPECTED_COLS)

    # Ensure all expected cols exist in output
    for c in EXPECTED_COLS:
        if c not in out_df.columns:
            out_df[c] = ""

    # Track existing domains
    existing_domains = set(out_df["domain"].astype(str).str.strip())

    # Build sampling pools for random fill
    sampling_pools = {}
    for col in EXPECTED_COLS:
        pool = out_df[col].astype(str).replace("", np.nan).dropna().tolist()
        sampling_pools[col] = pool

    # Process input in chunks
    chunk_size = 500
    chunk_iter = pd.read_csv(infile, dtype=str, chunksize=chunk_size)

    for chunk_idx, in_df in enumerate(chunk_iter, start=1):
        in_df = in_df.fillna("")  # ✅ FIX applied here
        print(f"\n📦 Processing chunk {chunk_idx} (rows {chunk_idx*chunk_size - chunk_size}–{chunk_idx*chunk_size - 1})")

        # Ensure input has required cols
        if "domain" not in in_df.columns or "nslookup" not in in_df.columns:
            raise ValueError("Input CSV must have 'domain' and 'nslookup' columns.")

        # Filter missing domains in this chunk
        in_df["domain"] = in_df["domain"].astype(str).str.strip()
        missing_domains = [d for d in in_df["domain"] if d and d not in existing_domains]

        if not missing_domains:
            print(f"⏩ No new domains in chunk {chunk_idx}.")
            continue

        print(f"✅ {len(missing_domains)} new domains found in chunk {chunk_idx}.")

        # Build new rows
        new_rows = []
        for dom in missing_domains:
            ns_val = in_df.loc[in_df["domain"] == dom, "nslookup"]
            ns_val = ns_val.iloc[0] if not ns_val.empty else ""

            row = {"domain": dom, "nslookup": ns_val}
            for col in ["ip","cpes","hostnames","ports","tags","vulns"]:
                pool = sampling_pools.get(col, [])
                row[col] = np.random.choice(pool) if pool else ""
            new_rows.append(row)
            existing_domains.add(dom)  # mark as processed

        # Save immediately in append mode
        new_df = pd.DataFrame(new_rows, columns=EXPECTED_COLS)
        write_header = not os.path.exists(outfile) or os.path.getsize(outfile) == 0
        new_df.to_csv(outfile, mode="a", header=write_header, index=False)

        print(f"💾 Wrote {len(new_rows)} rows from chunk {chunk_idx} to '{outfile}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile", "-i", default="combined_nslookup.csv")
    parser.add_argument("--outfile", "-o", default="shodan_results.csv")
    parser.add_argument("--seed", "-s", type=int, default=None)
    args = parser.parse_args()

    main(args.infile, args.outfile, args.seed)
