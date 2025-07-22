import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# DB config
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '157.90.17.11',
    'database': 'domain_data'
}

# Path to uploaded file
csv_file = 'db_excel_ns.csv'

# Expected column list
columns = [
    'domain', 'tld', 'registrar', 'whois_server', 'creation_date',
    'updated_date', 'expiration_date', 'status', 'registrant_name',
    'registrant_org', 'registrant_country', 'admin_email', 'tech_email',
    'name_servers', 'dnssec', 'lookup_date', 'source', 'domain_count',
    'nslookupA', 'nslookupAAAA', 'nslookupCNAME', 'nslookupMX',
    'nslookupNS', 'nslookupPTR', 'nslookupTXT', 'nslookupSRV', 'nslookupSOA'
]

# Load CSV
df = pd.read_csv(csv_file)

# Trim to only valid columns (ignore extras)
df = df[[col for col in columns if col in df.columns]]

# Fill missing columns if needed
for col in columns:
    if col not in df.columns:
        df[col] = None

# Convert everything to string and strip, then replace invalids
df = df.astype(str).applymap(lambda x: x.strip() if isinstance(x, str) else x)
df.replace(to_replace=['', 'nan', 'NaN', 'None', 'NULL'], value=None, inplace=True)

# Fix domain_count as INT
df['domain_count'] = pd.to_numeric(df['domain_count'], errors='coerce')

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nslookup (
            domain VARCHAR(255) PRIMARY KEY,
            tld VARCHAR(50),
            registrar TEXT,
            whois_server TEXT,
            creation_date VARCHAR(100),
            updated_date VARCHAR(100),
            expiration_date VARCHAR(100),
            status TEXT,
            registrant_name TEXT,
            registrant_org TEXT,
            registrant_country VARCHAR(100),
            admin_email TEXT,
            tech_email TEXT,
            name_servers TEXT,
            dnssec VARCHAR(50),
            lookup_date VARCHAR(100),
            source VARCHAR(100),
            domain_count INT,
            nslookupA TEXT,
            nslookupAAAA TEXT,
            nslookupCNAME TEXT,
            nslookupMX TEXT,
            nslookupNS TEXT,
            nslookupPTR TEXT,
            nslookupTXT TEXT,
            nslookupSRV TEXT,
            nslookupSOA TEXT
        )
    ''')

    insert_query = f'''
        INSERT IGNORE INTO nslookup ({','.join(columns)}) 
        VALUES ({','.join(['%s'] * len(columns))})
    '''

    for _, row in df.iterrows():
        values = [row.get(col) if row.get(col) not in ['nan', 'NaN', 'None', 'NULL', ''] else None for col in columns]
        try:
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"⚠️ Failed to insert domain {row.get('domain')}: {e}")

    conn.commit()
    print("✅ All rows inserted (duplicates ignored).")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("🔐 Access denied: Check DB user/password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("❌ Database not found.")
    else:
        print(f"⚠️ MySQL error: {err}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
