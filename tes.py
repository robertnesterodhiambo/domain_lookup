import mysql.connector
import pandas as pd

# === CONFIGURATION ===
CSV_FILE = 'tes_accesibilty.csv'
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '157.90.17.11',
    'database': 'domain_data'
}

# === COLUMN NAMES IN ORDER ===
columns = [
    'domain', 'tld', 'registrar', 'whois_server', 'creation_date', 'updated_date', 'expiration_date',
    'status', 'registrant_name', 'registrant_org', 'registrant_country', 'admin_email', 'tech_email',
    'name_servers', 'dnssec', 'lookup_date', 'source', 'subdomain_count',
    'nslookupA', 'nslookupAAAA', 'nslookupCNAME', 'nslookupMX', 'nslookupNS', 'nslookupPTR',
    'nslookupTXT', 'nslookupSRV', 'nslookupSOA',
    'Violations', 'Passes', 'Incomplete', 'Inapplicable'
]

# === READ CSV ===
df = pd.read_csv(CSV_FILE)
df = df[columns]  # Ensure correct column order

# === CONNECT TO DATABASE ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === CREATE TABLE IF NOT EXISTS ===
create_query = f"""
CREATE TABLE IF NOT EXISTS axxes (
    domain VARCHAR(255),
    tld VARCHAR(50),
    registrar VARCHAR(255),
    whois_server VARCHAR(255),
    creation_date VARCHAR(50),
    updated_date VARCHAR(50),
    expiration_date VARCHAR(50),
    status TEXT,
    registrant_name VARCHAR(255),
    registrant_org VARCHAR(255),
    registrant_country VARCHAR(50),
    admin_email VARCHAR(255),
    tech_email VARCHAR(255),
    name_servers TEXT,
    dnssec VARCHAR(20),
    lookup_date DATE,
    source VARCHAR(255),
    subdomain_count INT,
    nslookupA TEXT,
    nslookupAAAA TEXT,
    nslookupCNAME TEXT,
    nslookupMX TEXT,
    nslookupNS TEXT,
    nslookupPTR TEXT,
    nslookupTXT TEXT,
    nslookupSRV TEXT,
    nslookupSOA TEXT,
    Violations INT,
    Passes INT,
    Incomplete INT,
    Inapplicable INT
)
"""
cursor.execute(create_query)

# === INSERT DATA ROW BY ROW ===
insert_query = f"""
INSERT INTO axxes ({', '.join(columns)})
VALUES ({', '.join(['%s'] * len(columns))})
"""

row_count = 0
for row in df.itertuples(index=False, name=None):
    try:
        cursor.execute(insert_query, row)
        row_count += 1
    except Exception as e:
        print(f"Skipping row due to error: {e}")

conn.commit()
print(f"✅ Inserted {row_count} rows into 'axxes' table.")

# === CLEANUP ===
cursor.close()
conn.close()
