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

CSV_FILE = "shodan_results.csv"
TABLE_NAME = "finalboss"

NEW_COLUMNS = {
    "ip": "TEXT",
    "cpes": "TEXT",
    "hostnames": "TEXT",
    "ports": "TEXT",
    "tags": "TEXT",
    "vulns": "TEXT",
}

def ensure_columns(cursor):
    added = []
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
            domain = (row.get("domain") or "").strip()
            if not domain or domain in seen:
                continue
            seen.add(domain)
            rows.append(row)
    return rows

def update_and_insert(conn, cursor, rows):
    processed, updated, inserted = 0, 0, 0
    batch = []

    for row in rows:
        processed += 1
        domain = (row.get("domain") or "").strip()
        if not domain:
            continue

        # Prepare update fields
        set_parts = []
        params = []
        for col in NEW_COLUMNS.keys():
            val = row.get(col)
            if val:
                val = val.strip()
                if val != "":
                    set_parts.append(f"`{col}` = %s")
                    params.append(val)

        params.append(domain)
        sql = f"UPDATE `{TABLE_NAME}` SET {', '.join(set_parts)} WHERE `domain` = %s"
        cursor.execute(sql, params)

        if cursor.rowcount > 0:
            updated += 1
        else:
            # Prepare for insert
            insert_data = {"domain": domain}
            for col in NEW_COLUMNS.keys():
                insert_data[col] = (row.get(col) or "").strip() or None
            batch.append(insert_data)

        # Insert in chunks of 500
        if len(batch) >= 500:
            inserted += bulk_insert(cursor, batch)
            conn.commit()
            batch = []

        if processed % 500 == 0:
            conn.commit()

    # Insert any remaining
    if batch:
        inserted += bulk_insert(cursor, batch)
        conn.commit()

    return processed, updated, inserted

def bulk_insert(cursor, batch):
    """Insert rows in bulk into finalboss."""
    if not batch:
        return 0
    cols = ["domain"] + list(NEW_COLUMNS.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{TABLE_NAME}` ({', '.join(cols)}) VALUES ({placeholders})"
    values = []
    for row in batch:
        values.append(tuple(row[col] for col in cols))
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

    print("Updating and inserting...")
    processed, updated, inserted = update_and_insert(conn, cursor, rows)
    print(f"CSV rows processed: {processed}")
    print(f"Rows updated: {updated}")
    print(f"Rows inserted: {inserted}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
