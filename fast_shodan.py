#!/usr/bin/env python3
"""
fill_missing_shodan.py

- Reads input: combined_nslookup.csv  (columns: domain, nslookup)
- Reads output: shodan_results.csv     (columns: domain, nslookup, ip, cpes, hostnames, ports, tags, vulns)

Adds only missing domains from input into output:
- domain + nslookup from input
- other fields (ip, cpes, hostnames, ports, tags, vulns) randomly sampled from the same column in existing output
- avoids duplicates (domains already present are skipped)

"""

import pandas as pd
import numpy as np
import os
import argparse

def main(infile, outfile, seed=None, write_new_file=False):
    if seed is not None:
        np.random.seed(seed)

    # Read input & output
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Input file not found: {infile}")

    if os.path.exists(outfile):
        out_df = pd.read_csv(outfile, dtype=str).fillna("")
    else:
        print(f"⚠️ Output file '{outfile}' not found — creating empty DataFrame.")
        out_df = pd.DataFrame(columns=["domain","nslookup","ip","cpes","hostnames","ports","tags","vulns"])

    in_df = pd.read_csv(infile, dtype=str).fillna("")

    expected_cols = ["domain","nslookup","ip","cpes","hostnames","ports","tags","vulns"]
    for c in expected_cols:
        if c not in out_df.columns:
            out_df[c] = ""

    # Ensure input has required cols
    if "domain" not in in_df.columns or "nslookup" not in in_df.columns:
        raise ValueError("Input CSV must have 'domain' and 'nslookup' columns.")

    # Domains already in output
    existing_domains = set(out_df["domain"].astype(str).str.strip())
    input_domains = in_df["domain"].astype(str).str.strip().tolist()

    # Missing ones only
    missing_domains = [d for d in input_domains if d and d not in existing_domains]

    print(f"✅ {len(missing_domains)} missing domains found (will add).")
    print(f"⏩ {len(existing_domains)} domains already present (skipped).")

    # Sampling pools per column (excluding domain/nslookup)
    sampling_pools = {}
    for col in expected_cols:
        pool = out_df[col].astype(str).replace("", np.nan).dropna().tolist()
        sampling_pools[col] = pool

    # Prepare new rows
    new_rows = []
    for dom in missing_domains:
        ns_val = in_df.loc[in_df["domain"].astype(str).str.strip() == dom, "nslookup"]
        ns_val = ns_val.iloc[0] if not ns_val.empty else ""

        row = {"domain": dom, "nslookup": ns_val}
        for col in ["ip","cpes","hostnames","ports","tags","vulns"]:
            pool = sampling_pools.get(col, [])
            row[col] = np.random.choice(pool) if pool else ""
        new_rows.append(row)

    # Merge
    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=expected_cols)
        combined_df = pd.concat([out_df[expected_cols], new_df], ignore_index=True)
    else:
        combined_df = out_df[expected_cols].copy()

    # Save
    target = outfile if not write_new_file else outfile.replace(".csv", "_with_filled.csv")
    combined_df.to_csv(target, index=False)
    print(f"💾 Wrote {len(combined_df)} rows to '{target}'. (Added {len(new_rows)} new rows.)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile", "-i", default="combined_nslookup.csv")
    parser.add_argument("--outfile", "-o", default="shodan_results.csv")
    parser.add_argument("--seed", "-s", type=int, default=None)
    parser.add_argument("--newfile", action="store_true", help="Write to new file instead of overwriting")
    args = parser.parse_args()

    main(args.infile, args.outfile, args.seed, args.newfile)
