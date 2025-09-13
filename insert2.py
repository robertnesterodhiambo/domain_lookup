#!/usr/bin/env python3
"""
Load complete.csv into MySQL/MariaDB:
- table name = filename without extension (sanitized)
- columns = CSV headers (sanitized)
- adds tld column extracted from domain
- domain is UNIQUE to avoid duplicates
- inserts in chunks of CHUNK_SIZE with progress
"""

import os
import math
import re
import csv
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# Optional progress bar - fallback to prints if tqdm not available
try:
    from tqdm import tqdm
    TQDM = True
except Exception:
    TQDM = False

# ---------- Config ----------
DB_CONFIG = {
    'user': 'serviceapp',
    'password': '97HVhkvT4Zw3vd6q9uAgVYiJhsWBFz',
    'host': '46.62.140.165',
    'database': 'complete'      # will be created if missing
}

CSV_FILE = "complete.csv"
CHUNK_SIZE = 500
# ----------------------------

def sanitize_identifier(name: str) -> str:
    """Make a safe SQL identifier from arbitrary header text."""
    n = (name or "").strip()
    n = re.sub(r'\s+', '_', n)            # spaces -> underscore
    n = re.sub(r'[^\w]', '', n)           # remove anything not [A-Za-z0-9_]
    if not n:
        n = "col"
    if re.match(r'^\d', n):
        n = 'c_' + n
    return n.lower()

def safe_table_name(fname: str) -> str:
    base = os.path.splitext(os.path.basename(fname))[0]
    base = re.sub(r'[^\w]', '_', base).lower()
    if not base:
        base = "table1"
    return base

# --- Load CSV (keep everything as strings; ensure quoting works for commas in fields) ---
print("📥 Reading CSV...")
df = pd.read_csv(
    CSV_FILE,
    dtype=str,                # read all as strings (safer for dynamic tables)
    keep_default_na=False,    # don't turn empty strings to NaN
    na_filter=False,          # keep blanks as empty strings
    quoting=csv.QUOTE_MINIMAL,
    encoding='utf-8',
    on_bad_lines='warn'
)

# Strip header whitespace
df.columns = [c.strip() for c in df.columns]

# locate domain column (case-insensitive)
domain_col = None
for c in df.columns:
    if c.strip().lower() == "domain":
        domain_col = c
        break

if domain_col is None:
    raise SystemExit("❌ No 'domain' column found in CSV headers (case-insensitive).")

# add tld column (everything after last dot; empty string if none)
df["tld"] = df[domain_col].astype(str).apply(lambda x: x.rsplit('.', 1)[-1] if '.' in x else "")

# --- Sanitize column names and ensure uniqueness ---
orig_cols = list(df.columns)
sanitized_map = {}   # original -> sanitized
seen = {}
for col in orig_cols:
    s = sanitize_identifier(col)
    if s in seen:
        seen[s] += 1
        s = f"{s}_{seen[s]}"
    else:
        seen[s] = 0
    sanitized_map[col] = s

# rename DataFrame to sanitized column names for insertion
df_sanitized = df.rename(columns=sanitized_map)

# build list of sanitized columns in the original order
sanitized_cols = [sanitized_map[c] for c in orig_cols]

# safe table name
table_name = safe_table_name(CSV_FILE)

# --- Connect to MySQL and create database/table ---
print("🔌 Connecting to MySQL...")
conn = mysql.connector.connect(
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"]
)
cursor = conn.cursor()

# create database if missing (use utf8mb4)
cursor.execute(
    f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}` "
    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
)
conn.database = DB_CONFIG['database']

# prepare CREATE TABLE columns
cols_defs = []
domain_safe = sanitized_map[domain_col]
tld_safe = sanitized_map["tld"]  # we added this above

for orig in orig_cols:
    safe = sanitized_map[orig]
    if safe == domain_safe:
        cols_defs.append(f"`{safe}` VARCHAR(255) NOT NULL")
    elif safe == tld_safe:
        cols_defs.append(f"`{safe}` VARCHAR(64)")
    else:
        cols_defs.append(f"`{safe}` TEXT")

cols_section = ",\n  ".join(cols_defs)

create_table_sql = (
    f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
    f"  {cols_section},\n"
    f"  UNIQUE KEY `uniq_domain` (`{domain_safe}`)\n"
    f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
)

print("🛠 Creating table (if not exists)...")
cursor.execute(create_table_sql)

# --- Prepare insert statement ---
cols_quoted = ", ".join(f"`{c}`" for c in sanitized_cols)
placeholders = ", ".join(["%s"] * len(sanitized_cols))
insert_sql = f"INSERT IGNORE INTO `{table_name}` ({cols_quoted}) VALUES ({placeholders})"

# --- Insert in chunks with progress ---
total_rows = len(df_sanitized)
if total_rows == 0:
    print("⚠️ CSV contains no rows. Nothing to insert.")
else:
    total_chunks = math.ceil(total_rows / CHUNK_SIZE)
    print(f"📦 Inserting {total_rows} rows into `{table_name}` in {total_chunks} chunks (chunk size: {CHUNK_SIZE})...")

    if TQDM:
        outer = tqdm(range(0, total_rows, CHUNK_SIZE), desc="chunks")
    else:
        outer = range(0, total_rows, CHUNK_SIZE)

    inserted_rows = 0
    chunk_no = 0
    for start in outer:
        chunk_no += 1
        chunk_df = df_sanitized.iloc[start:start + CHUNK_SIZE]
        # convert to list of tuples; keep empty strings as-is
        data = []
        for row in chunk_df[sanitized_cols].itertuples(index=False, name=None):
            # replace empty string with None? we keep empty strings; DB TEXT allows it.
            data.append(tuple(None if (isinstance(v, float) and pd.isna(v)) else v for v in row))
        try:
            cursor.executemany(insert_sql, data)
            conn.commit()
            # cursor.rowcount is number of rows affected by last executemany call; may be -1 depending on driver
            inserted_rows += len(data)  # rough count (duplicates skipped by INSERT IGNORE)
            if not TQDM:
                print(f"✅ Inserted chunk {chunk_no}/{total_chunks} ({len(data)} rows)")
        except mysql.connector.Error as e:
            print(f"❌ Error inserting chunk {chunk_no}: {e}")
            # optional: break or continue; we'll continue
            continue

    print(f"🎉 Done. Attempted to insert {total_rows} rows (in {total_chunks} chunks).")
    print("Note: INSERT IGNORE was used, so rows with duplicate domain values were skipped by the DB.")

cursor.close()
conn.close()
