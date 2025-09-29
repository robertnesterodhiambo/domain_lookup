import pandas as pd
import mariadb

DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'complete'
}

# --- Connect to MariaDB ---
print("🔌 Connecting to MariaDB server...")
conn = mariadb.connect(**DB_CONFIG)
cursor = conn.cursor()
print("✅ Connected successfully!\n")

# --- Step 1: Fetch unique TLDs ---
print("📥 Fetching unique TLDs from domain_data.finalboss...")
cursor.execute("SELECT DISTINCT tld FROM domain_data.finalboss;")
tlds = [row[0] for row in cursor.fetchall()]
print(f"✅ Found {len(tlds)} unique TLDs: {tlds}\n")

# --- Process each TLD ---
for tld in tlds:
    print(f"🔎 Processing TLD: {tld}")

    # Step 2a: Try filtered query
    query_filtered = f"""
        SELECT *
        FROM domain_data.finalboss
        WHERE tld = '{tld}'
          AND registrant_name <> 'no data'
          AND reseller_name <> 'no data'
          AND nslookupA <> 'no data'
          AND nslookupAAAA <> 'no data'
          AND tags IS NOT NULL
        LIMIT 500;
    """
    cursor.execute(query_filtered)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    if not rows:
        print(f"   ⚠️ No rows with filters for TLD {tld}, retrying without filters...")
        query_unfiltered = f"""
            SELECT *
            FROM domain_data.finalboss
            WHERE tld = '{tld}'
            LIMIT 500;
        """
        cursor.execute(query_unfiltered)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

    if not rows:
        print(f"   ❌ No rows found at all for TLD {tld}, skipping...\n")
        continue

    df_finalboss = pd.DataFrame(rows, columns=colnames)
    print(f"   ✅ Retrieved {len(df_finalboss)} rows from finalboss")

    # Step 3: Only fetch related rows from complete.complete
    domains = df_finalboss["domain"].tolist()
    domains_str = ",".join([f"'{d}'" for d in domains])

    query_complete = f"""
        SELECT domain, Apache, HTTPServer, RedirectLocation, Title, Bootstrap,
               Email, JQuery, `Meta-Author`, `Open-Graph-Protocol`, PoweredBy,
               Script, `Strict-Transport-Security`, UncommonHeaders, WordPress,
               `X-Frame-Options`, ip_web, country_web
        FROM complete.complete
        WHERE domain IN ({domains_str});
    """
    cursor.execute(query_complete)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df_complete = pd.DataFrame(rows, columns=colnames)
    print(f"   ✅ Retrieved {len(df_complete)} rows from complete.complete")

    # Step 4: Merge only relevant rows
    merged_df = pd.merge(df_finalboss, df_complete, on="domain", how="left")

    # Step 5: Deduplicate and limit to 500
    merged_df = merged_df.drop_duplicates(subset=["domain"])
    merged_df = merged_df.head(500)
    print(f"   ✅ Merge complete, {len(merged_df)} rows after deduplication")

    # Step 6: Save to CSV
    filename = f"{tld}.csv"
    merged_df.to_csv(filename, index=False, quoting=1)
    print(f"   💾 Saved {filename} ({len(merged_df)} rows)\n")

print("🎉 All TLDs processed successfully!")
cursor.close()
conn.close()
