#!/usr/bin/env python3
import csv
import mysql.connector

DB_CONFIG = {
    'user': 'serviceapp',
    'password': '97HVhkvT4Zw3vd6q9uAgVYiJhsWBFz',
    'host': '46.62.140.165',
    'database': 'domain_data'
}

CSV_FILE = "whatweb_results.csv"
TABLE_NAME = "finalboss"

# Define the columns to add/update (excluding ip_web and country_web)
NEW_COLUMNS = {
    "Apache": "TEXT",
    "HTTPServer": "TEXT",
    "RedirectLocation": "TEXT",
    "Title": "TEXT",
    "Bootstrap": "TEXT",
    "Email": "TEXT",
    "JQuery": "TEXT",
    "Meta-Author": "TEXT",
    "Open-Graph-Protocol": "TEXT",
    "PoweredBy": "TEXT",
    "Script": "TEXT",
    "Strict-Transport-Security": "TEXT",
    "UncommonHeaders": "TEXT",
    "WordPress": "TEXT",
    "X-Frame-Options": "TEXT",
}

def ensure_columns(cursor):
    added = []
    for col, coltype in NEW_COLUMNS.items():
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
            """,
            (DB_CONFIG['database'], TABLE_NAME, col)
        )
        exists = cursor.fetchone()[0] > 0

        if not exists:
            print(f"Adding missing column: {col}")
            cursor.execute(f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `{col}` {coltype} DEFAULT NULL")
            added.append(col)
    return added

def normalize_domain(target):
    """Extract plain domain from Target (strip http://, https://, trailing /)."""
    if not target:
        return None
    target = target.strip()
    if target.startswith("http://"):
        target = target[7:]
    elif target.startswith("https://"):
        target = target[8:]
    return target.rstrip("/")

def load_csv_unique():
    """Load CSV and keep only first occurrence of each domain."""
    seen = set()
    rows = []
    with open(CSV_FILE, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            domain = normalize_domain(row.get("Target"))
            if not domain or domain in seen:
                continue
            seen.add(domain)
            row["domain"] = domain  # store cleaned domain
            rows.append(row)
    return rows

def update_and_insert(conn, cursor, rows):
    processed, inserted, failed = 0, 0, 0
    big_batch = []

    for row in rows:
        processed += 1
        domain = row.get("domain")
        if not domain:
            failed += 1
            print(f"⚠️ Skipping row with missing domain at row {processed}")
            continue

        insert_data = {"domain": domain}
        for col in NEW_COLUMNS.keys():
            insert_data[col] = (row.get(col) or "").strip() or None

        insert_data["ip_web"] = (row.get("IP") or "").strip() or None
        insert_data["country_web"] = (row.get("Country") or "").strip() or None

        big_batch.append(insert_data)

        if len(big_batch) >= 500:
            ok, fail = process_big_batch(conn, cursor, big_batch)
            inserted += ok
            failed += fail
            print(f"✅ Processed chunk of 500 rows (total processed: {processed}, failed so far: {failed})")
            big_batch = []

    if big_batch:
        ok, fail = process_big_batch(conn, cursor, big_batch)
        inserted += ok
        failed += fail
        print(f"✅ Processed final chunk ({len(big_batch)} rows, total processed: {processed}, failed total: {failed})")

    return processed, inserted, failed

def process_big_batch(conn, cursor, big_batch):
    inserted, failed = 0, 0
    for i in range(0, len(big_batch), 100):
        sub_batch = big_batch[i:i+100]
        try:
            inserted += bulk_insert(cursor, sub_batch)
            conn.commit()
            print(f"   ↳ Inserted/updated subchunk {i//100 + 1} of {(len(big_batch) + 99)//100} ({len(sub_batch)} rows)")
        except Exception as e:
            conn.rollback()
            failed += len(sub_batch)
            print(f"❌ Failed subchunk {i//100 + 1}: {e} (skipped {len(sub_batch)} rows)")
    return inserted, failed

def bulk_insert(cursor, batch):
    """Insert or update rows in bulk using ON DUPLICATE KEY UPDATE."""
    if not batch:
        return 0

    cols = ["domain", "ip_web", "country_web"] + list(NEW_COLUMNS.keys())
    placeholders = ", ".join(["%s"] * len(cols))

    # Wrap every column with backticks for safety
    col_names = ", ".join([f"`{col}`" for col in cols])

    sql = f"""
    INSERT INTO `{TABLE_NAME}` ({col_names})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE
    {', '.join([f'`{col}`=VALUES(`{col}`)' for col in cols if col != "domain"])}
    """

    values = [tuple(row[col] for col in cols) for row in batch]
    cursor.executemany(sql, values)
    return len(values)

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("🔍 Ensuring columns exist...")
    added_cols = ensure_columns(cursor)
    conn.commit()
    print("   Added/verified columns:", ", ".join(added_cols) if added_cols else "none")

    print(f"📂 Loading CSV: {CSV_FILE}")
    rows = load_csv_unique()
    print(f"   Unique rows loaded: {len(rows)}")

    print("🚀 Inserting/updating in chunks...")
    processed, inserted, failed = update_and_insert(conn, cursor, rows)
    print(f"📊 CSV rows processed: {processed}")
    print(f"✅ Rows inserted/updated: {inserted}")
    print(f"❌ Rows failed/skipped: {failed}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
