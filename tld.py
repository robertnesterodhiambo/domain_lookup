import mysql.connector
import pandas as pd
import os

# --- Database connection details ---
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'complete'
}

# --- Output folder ---
OUTPUT_DIR = "tld_csv_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Connect to database ---
print("🔗 Connecting to database...")
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# --- Get unique TLDs ---
print("📌 Fetching unique TLDs...")
cursor.execute("SELECT DISTINCT tld FROM complete;")
tlds = [row[0] for row in cursor.fetchall()]
print(f"✅ Found {len(tlds)} unique TLDs: {tlds}")

# --- Process each TLD ---
for tld in tlds:
    print(f"\n📍 Processing TLD: {tld}")

    query = f"""
        SELECT *
        FROM complete c
        WHERE tld = %s
          AND `Open-Graph-Protocol` <> 'no data'
          AND PoweredBy <> 'no data'
        LIMIT 500;
    """
    df = pd.read_sql(query, conn, params=(tld,))

    if not df.empty:
        file_path = os.path.join(OUTPUT_DIR, f"{tld}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8")
        print(f"💾 Saved {len(df)} rows to {file_path}")
    else:
        print(f"⚠️ No data found for {tld}")

# --- Close connection ---
cursor.close()
conn.close()
print("\n🎉 All CSV files have been generated successfully!")

