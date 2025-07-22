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

# CSV path
csv_file = 'db_excel_ns.csv'
batch_size = 20000

# Expected columns
columns = [
    'domain', 'tld', 'registrar', 'whois_server', 'creation_date',
    'updated_date', 'expiration_date', 'status', 'registrant_name',
    'registrant_org', 'registrant_country', 'admin_email', 'tech_email',
    'name_servers', 'dnssec', 'lookup_date', 'source', 'domain_count',
    'nslookupA', 'nslookupAAAA', 'nslookupCNAME', 'nslookupMX',
    'nslookupNS', 'nslookupPTR', 'nslookupTXT', 'nslookupSRV', 'nslookupSOA'
]

# Load and clean CSV
df = pd.read_csv(csv_file)

# Trim extra cols and fill missing ones
df = df[[col for col in columns if col in df.columns]]
for col in columns:
    if col not in df.columns:
        df[col] = None

# Clean data
df = df.astype(str).applymap(lambda x: x.strip() if isinstance(x, str) else x)
df.replace(['', 'nan', 'NaN', 'None', 'NULL'], None, inplace=True)
df['domain_count'] = pd.to_numeric(df['domain_count'], errors='coerce')

# Create SQL insert query
insert_query = f'''
    INSERT IGNORE INTO nslookup ({','.join(columns)}) 
    VALUES ({','.join(['%s'] * len(columns))})
'''

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table if not exists
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

    total_success = 0
    total_failed = 0
    total_blocks = (len(df) // batch_size) + 1

    for block_num in range(total_blocks):
        start = block_num * batch_size
        end = start + batch_size
        df_block = df.iloc[start:end]

        success_count = 0
        fail_count = 0

        for _, row in df_block.iterrows():
            values = [row.get(col) if row.get(col) not in ['nan', 'NaN', 'None', 'NULL', ''] else None for col in columns]
            try:
                cursor.execute(insert_query, values)
                success_count += 1
            except Exception as e:
                print(f"⚠️ Failed to insert domain {row.get('domain')}: {e}")
                fail_count += 1

        conn.commit()
        print(f"✅ Block {block_num + 1}/{total_blocks} done — Success: {success_count}, Failed: {fail_count}")
        total_success += success_count
        total_failed += fail_count

    print(f"\n✅ Insertion complete.")
    print(f"✅ Total successful inserts: {total_success}")
    print(f"❌ Total failed inserts: {total_failed}")

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
    if 'conn' in locals() and conn.is_connected():
        conn.close()
