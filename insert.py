import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os

DB_CONFIG = {
    'user': 'serviceapp',
    'password': '97HVhkvT4Zw3vd6q9uAgVYiJhsWBFz',
    'host': '46.62.140.165',
    'database': 'domain_data'
}

TABLE_NAME = 'finalboss'
INPUT_FILE = 'complete.csv'
CHUNKSIZE = 50000     # how many rows pandas reads at once
BATCH_SIZE = 100    # how many rows we insert per executemany
PROGRESS_FILE = 'insert_progress.txt'


def sanitize_column_name(name):
    return name.strip().replace(' ', '_').replace('-', '_')


def create_table_from_df(cursor, df):
    columns = []
    for col in df.columns:
        col_clean = sanitize_column_name(col)
        columns.append(f"`{col_clean}` TEXT")

    if 'domain' not in df.columns:
        raise ValueError("CSV must contain a 'domain' column.")

    table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        {', '.join(columns)},
        UNIQUE KEY (domain)
    )
    """
    cursor.execute(table_sql)


def insert_chunk(cursor, df, batch_size=BATCH_SIZE):
    cols = ', '.join(f"`{sanitize_column_name(col)}`" for col in df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT IGNORE INTO `{TABLE_NAME}` ({cols}) VALUES ({placeholders})"

    values = [
        tuple(str(v) if pd.notna(v) else "no data" for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        cursor.executemany(insert_sql, batch)


def get_last_processed_chunk():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return int(f.read().strip())
    return -1


def save_progress(chunk_number):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(chunk_number))


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    last_chunk_done = get_last_processed_chunk()
    print(f"▶️ Resuming from chunk {last_chunk_done + 1}")

    total_inserted = 0

    for chunk_number, chunk in enumerate(pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE, quotechar='"', on_bad_lines='skip')):
        if chunk_number <= last_chunk_done:
            continue  # skip already done

        chunk.columns = [sanitize_column_name(col) for col in chunk.columns]
        chunk = chunk.fillna("no data")

        if chunk_number == 0:
            create_table_from_df(cursor, chunk)

        try:
            insert_chunk(cursor, chunk)
            conn.commit()
            total_inserted += len(chunk)
            save_progress(chunk_number)
            print(f"✅ Chunk {chunk_number} inserted ({len(chunk)} rows). Total so far: {total_inserted}")
        except mysql.connector.Error as err:
            conn.rollback()
            print(f"⚠️ Error inserting chunk {chunk_number}: {err}")

    cursor.close()
    conn.close()
    print("🎉 All data inserted successfully!")


if __name__ == "__main__":
    main()

