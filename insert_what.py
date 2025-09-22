#!/usr/bin/env python3
import csv
import mysql.connector
from mysql.connector import errorcode

DB_CONFIG = {
    'user': 'serviceapp',
    'password': '97HVhkvT4Zw3vd6q9uAgVYiJhsWBFz',
    'host': '46.62.140.165',
    'database': 'domain_data'
}

CSV_FILE = "whatweb_results.csv"
TABLE_NAME = "finalboss"

# Define the columns to add/update (excluding IP and Country which map to existing columns)
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
    # Ensure the table has all required columns (excluding ip_web and country_web)
    for col, coltype in NEW_COLUMNS.items():
        try:
            cursor.execute(f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN IF NOT EXISTS `{col}` {coltype} DEFAULT NULL")
            added.append(col)
        except mysql.connector.Error as e:
            if e.errno in (errorcode.ER_PARSE_ERROR,):
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
                    """,
                    (DB_CONFIG['database'], TABLE_NAME, col)
                )
                exists = cursor.fetchone()[0] > 0
                if not exists:
                    cursor.execute(f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `{col}` {coltype} DEFAULT NULL")
                    added.append(col)
            else:
                raise
    return added

def load_csv_unique():
    """Load CSV and keep only first occurrence of each domain."""
    seen = set()
    rows = []
    with open(CSV_FILE, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            domain = (row.get("Target") or "").strip()
            if not domain or domain in seen:
                continue
            seen.add(domain)
            rows.append(row)
    return rows

def update_and_insert(conn, cursor, rows):
    processed, inserted = 0, 0
    big_batch = []

    for row in rows:
        processed += 1
        domain = (row.get("Target") or "").strip()
        if not domain:
            continue

        # Map CSV data to insert_data dictionary
        insert_data = {"domain": domain}
        for col in NEW_COLUMNS.keys():
            insert_data[col] = (row.get(col) or "").strip() or None

        # Map IP and Country to existing table columns
        insert_data["ip_web"] = (row.get("IP") or "").strip() or None
        insert_data["country_web"] = (row.get("Country") or "").strip() or None

        big_batch.append(insert_data)

        # When we hit 500 rows, process them in chunks of 100
        if len(big_batch) >= 500:
            inserted += process_big_batch(conn, cursor, big_batch)
            print(f"✅ Processed chunk of 500 rows (total processed: {processed})")
            big_batch = []

    # Handle remaining rows
    if big_batch:
        inserted += process_big_batch(conn, cursor, big_batch)
        print(f"✅ Processed final chunk ({len(big_batch)} rows, total processed: {processed})")

    return processed, inserted

def process_big_batch(conn, cursor, big_batch):
    """Split a 500-row batch into 100-row inserts with commit per subchunk."""
    inserted = 0
    for i in range(0, len(big_batch), 100):
        sub_batch = big_batch[i:i+100]
        inserted += bulk_insert(cursor, sub_batch)
        conn.commit()
        print(f"   ↳ Inserted/updated subchunk {i//100 + 1} of {len(big_batch)//100 + 1} ({len(sub_batch)} rows)")
    return inserted

def bulk_insert(cursor, batch):
    """Insert or update rows in bulk using ON DUPLICATE KEY UPDATE."""
    if not batch:
        return 0

    # Use ip_web and country_web instead of IP and Country
    cols = ["domain", "ip_web", "country_web"] + list(NEW_COLUMNS.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
    INSERT INTO `{TABLE_NAME}` ({', '.join(cols)})
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

    print("Ensuring columns exist...")
    added_cols = ensure_columns(cursor)
    conn.commit()
    print("Added/verified columns:", ", ".join(added_cols) if added_cols else "none")

    print(f"Loading CSV: {CSV_FILE}")
    rows = load_csv_unique()
    print(f"Unique rows loaded: {len(rows)}")

    print("Inserting/updating in chunks...")
    processed, inserted = update_and_insert(conn, cursor, rows)
    print(f"CSV rows processed: {processed}")
    print(f"Rows inserted/updated: {inserted}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
