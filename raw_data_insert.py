import pandas as pd
import mysql.connector
from tqdm import tqdm

# DB CONFIG
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '157.90.17.11',
    'database': 'domain_data'
}

CSV_FILE = 'data_rdap.csv'
TABLE_NAME = 'raw_data'
CHUNK_SIZE = 100_000
UNIQUE_COLUMN = 'domain'

# Connect to DB
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Utility: create table dynamically from column names
def create_table(columns):
    col_defs = []
    for col in columns:
        col_clean = col.strip().replace(' ', '_').lower()
        col_defs.append(f"`{col_clean}` TEXT")
    col_defs.append(f"UNIQUE KEY (`{UNIQUE_COLUMN}`)")
    col_defs_str = ", ".join(col_defs)

    cursor.execute(f"DROP TABLE IF EXISTS `{TABLE_NAME}`")
    cursor.execute(f"CREATE TABLE `{TABLE_NAME}` ({col_defs_str})")
    conn.commit()

# Utility: insert one chunk
def insert_chunk(chunk):
    # Clean and normalize column names
    chunk.columns = [c.strip().replace(' ', '_').lower() for c in chunk.columns]

    # Ensure unique column is present and has no missing values
    if UNIQUE_COLUMN not in chunk.columns:
        raise ValueError(f"Missing required unique column: '{UNIQUE_COLUMN}'")
    chunk = chunk[chunk[UNIQUE_COLUMN].notna()]

    if chunk.empty:
        return

    placeholders = ', '.join(['%s'] * len(chunk.columns))
    cols = ', '.join([f'`{c}`' for c in chunk.columns])
    sql = f"INSERT IGNORE INTO `{TABLE_NAME}` ({cols}) VALUES ({placeholders})"

    # Replace NaNs with None, convert all values to string or None
    data = [
        tuple(None if pd.isna(cell) else str(cell) for cell in row)
        for row in chunk.to_numpy()
    ]

    cursor.executemany(sql, data)
    conn.commit()

# Main processing loop
first_chunk = True
for chunk in tqdm(pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE)):
    if first_chunk:
        create_table(chunk.columns)
        first_chunk = False
    insert_chunk(chunk)

# Create view that shows `.nl` domains first
cursor.execute("DROP VIEW IF EXISTS sorted_raw_data")
cursor.execute(f"""
    CREATE VIEW sorted_raw_data AS
    SELECT * FROM {TABLE_NAME}
    ORDER BY domain LIKE '%.nl' DESC, domain
""")
conn.commit()

# Test: fetch first 10 entries from the sorted view
print("\nTop 10 domains (with .nl first):\n")
cursor.execute("SELECT domain FROM sorted_raw_data LIMIT 10")
for row in cursor.fetchall():
    print(row[0])

# Cleanup
cursor.close()
conn.close()
