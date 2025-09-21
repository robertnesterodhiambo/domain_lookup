#!/usr/bin/env python3
"""
fast_what.py

- Reads input: combined_nslookup.csv  (columns: domain, nslookup)
- Reads output: whatweb_results.csv   (columns: Target, Apache, Country, HTTPServer, IP,
                                       RedirectLocation, Title, Bootstrap, Email, JQuery,
                                       Meta-Author, Open-Graph-Protocol, PoweredBy, Script,
                                       Strict-Transport-Security, UncommonHeaders, WordPress,
                                       X-Frame-Options)

Adds only missing domains from input into output:
- Extracts bare domain from Target
- Keeps all existing rows untouched
- For missing domains:
    - uses domain + nslookup from input
    - Target = "http://<domain>"
    - fills other columns with random values sampled from the same column in existing data
- Avoids duplicates
- Works efficiently with millions of rows
"""

import pandas as pd
import random
import argparse
import os
from urllib.parse import urlparse

def extract_domain(target: str) -> str:
    """Extract bare domain from URL in Target column."""
    if not isinstance(target, str) or not target.strip():
        return ""
    parsed = urlparse(target)
    return parsed.netloc or target.replace("http://", "").replace("https://", "").strip("/")

def main(infile, outfile, seed=None, write_new_file=False, chunksize=500000):
    if seed is not None:
        random.seed(seed)

    if not os.path.exists(infile):
        raise FileNotFoundError(f"Input file not found: {infile}")

    # Load output file if exists
    if os.path.exists(outfile):
        out_df = pd.read_csv(outfile, dtype=str).fillna("")
    else:
        print(f"⚠️ Output file '{outfile}' not found — creating empty DataFrame.")
        out_df = pd.DataFrame()

    # Input domains
    in_df = pd.read_csv(infile, dtype=str).fillna("")

    expected_cols = [
        "Target","Apache","Country","HTTPServer","IP","RedirectLocation","Title","Bootstrap",
        "Email","JQuery","Meta-Author","Open-Graph-Protocol","PoweredBy","Script",
        "Strict-Transport-Security","UncommonHeaders","WordPress","X-Frame-Options"
    ]

    # Ensure output has all expected columns
    for col in expected_cols:
        if col not in out_df.columns:
            out_df[col] = ""

    # Add/refresh domain column
    out_df["domain"] = out_df["Target"].apply(extract_domain)

    # Existing domains
    existing_domains = set(out_df["domain"].astype(str).str.strip())
    print(f"✅ Loaded {len(existing_domains)} existing domains from output.")

    input_domains = in_df["domain"].astype(str).str.strip().tolist()
    missing_domains = [d for d in input_domains if d and d not in existing_domains]

    print(f"✅ {len(missing_domains)} missing domains found.")
    print(f"⏩ {len(existing_domains)} domains already present (skipped).")

    # Sampling pools for each column
    sampling_pools = {}
    for col in expected_cols:
        pool = out_df[col].astype(str).replace("", pd.NA).dropna().tolist()
        sampling_pools[col] = pool

    # Process in chunks
    new_rows = []
    for i, dom in enumerate(missing_domains, 1):
        ns_val = in_df.loc[in_df["domain"].astype(str).str.strip() == dom, "nslookup"]
        ns_val = ns_val.iloc[0] if not ns_val.empty else ""

        row = {"domain": dom}
        row["Target"] = f"http://{dom}"
        for col in expected_cols:
            if col == "Target":
                continue
            pool = sampling_pools.get(col, [])
            row[col] = random.choice(pool) if pool else ""

        new_rows.append(row)

        # Flush every chunksize to save memory
        if len(new_rows) >= chunksize:
            new_df = pd.DataFrame(new_rows, columns=["domain"] + expected_cols)
            out_df = pd.concat([out_df[["domain"] + expected_cols], new_df], ignore_index=True)
            new_rows = []

    # Final flush
    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=["domain"] + expected_cols)
        out_df = pd.concat([out_df[["domain"] + expected_cols], new_df], ignore_index=True)

    # Save
    target = outfile if not write_new_file else outfile.replace(".csv", "_with_filled.csv")
    out_df.to_csv(target, index=False)
    print(f"💾 Wrote {len(out_df)} rows to '{target}'. (Added {len(missing_domains)} new rows.)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile", "-i", default="combined_nslookup.csv")
    parser.add_argument("--outfile", "-o", default="whatweb_results.csv")
    parser.add_argument("--seed", "-s", type=int, default=None)
    parser.add_argument("--newfile", action="store_true", help="Write to new file instead of overwriting")
    parser.add_argument("--chunksize", "-c", type=int, default=500000, help="Flush interval for large datasets")
    args = parser.parse_args()

    main(args.infile, args.outfile, args.seed, args.newfile, args.chunksize)
