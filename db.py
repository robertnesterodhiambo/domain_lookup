import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# --- Config ---
CSV_FILE = "subdomain.csv"
DB_NAME = "domains"
TABLE_NAME = "domain"
USER = "root"
PASSWORD = "root235"
HOST = "localhost"

# --- Load CSV and clean ---
df = pd.read_csv(CSV_FILE)

# Drop rows with empty or missing subdomain_count
df = df[df['subdomain_count'].notna()]
df = df[df['subdomain_count'].astype(str).str.strip() != '']

# Replace NaN with None for SQL compatibility
df = df.where(pd.notnull(df), None)
df = df.astype(object)
columns = df.columns.tolist()

# --- Connect to MariaDB server (no DB selected yet) ---
conn = mysql.connector.connect(
    user=USER,
    password=PASSWORD,
    host=HOST
)
cursor = conn.cursor()

# --- Create database if it doesn't exist ---
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
conn.database = DB_NAME

# --- Drop table if it already exists ---
cursor.execute(f"DROP TABLE IF EXISTS `{TABLE_NAME}`")

# --- Create table with fixed schema ---
create_sql = f"""
CREATE TABLE `{TABLE_NAME}` (
    domain TEXT,
    domain_name TEXT,
    registrar TEXT,
    creation_date TEXT,
    expiration_date TEXT,
    updated_date TEXT,
    status TEXT,
    name_servers TEXT,
    emails TEXT,
    country TEXT,
    city TEXT,
    tld TEXT,
    raw_text LONGTEXT,
    subdomain_count INT
);
"""
cursor.execute(create_sql)

# --- Insert each row ---
for _, row in df.iterrows():
    placeholders = ", ".join(["%s"] * len(row))
    insert_sql = f"INSERT INTO `{TABLE_NAME}` ({', '.join(f'`{c}`' for c in columns)}) VALUES ({placeholders})"
    cursor.execute(insert_sql, tuple(row))

# --- Commit and Close ---
conn.commit()
cursor.close()
conn.close()

print("✅ Table created, data cleaned, and CSV imported into 'domains.domain'.")
