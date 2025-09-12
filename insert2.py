import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os
import math

DB_CONFIG = {
    'user': 'serviceapp',
    'password': '97HVhkvT4Zw3vd6q9uAgVYiJhsWBFz',
    'host': '46.62.140.165',
    'database': 'complete'
}

CSV_FILE = "complete.csv"
TABLE_NAME = os.path.splitext(os.path.basename(CSV_FILE))[0]  # "complete"
CHUNK_SIZE = 500

# --- Load CSV ---
df = pd.read_csv(CSV_FILE)

# Add TLD column from domain
df["tld"] = df["domain"].astype(str).str.split(".").str[-1]

# --- Connect to MySQL ---
try:
    conn = mysql.connector.connect(
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"]
    )
    cursor = conn.cursor()

    # Create database if not exists
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    conn.database = DB_CONFIG["database"]

    # --- Create table dynamically ---
    columns = []
    for col in df.columns:
        if col == "domain":
            columns.append("domain VARCHAR(255) UNIQUE")
        else:
            columns.append(f"`{col}` TEXT")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {', '.join(columns)}
    )
    """
    cursor.execute(create_table_query)

    # --- Insert in chunks ---
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"""
        INSERT IGNORE INTO {TABLE_NAME} ({", ".join(df.columns)})
        VALUES ({placeholders})
    """

    total_rows = len(df)
    total_chunks = math.ceil(total_rows / CHUNK_SIZE)
    print(f"📦 Inserting {total_rows} rows into '{TABLE_NAME}' in {total_chunks} chunks...")

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = df.iloc[i:i+CHUNK_SIZE]
        data_tuples = [tuple(row) for row in chunk.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        print(f"✅ Inserted chunk {i//CHUNK_SIZE + 1}/{total_chunks} ({len(chunk)} rows)")

    print("🎉 All data inserted successfully.")

    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("❌ Invalid username/password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("❌ Database does not exist and could not be created")
    else:
        print(err)
