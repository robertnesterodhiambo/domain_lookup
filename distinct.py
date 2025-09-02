import mysql.connector
import pandas as pd

# Database configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'domain_data'
}

# SQL query
query = """
SELECT DISTINCT
    nslookupA,
    city,
    region,
    country,
    loc,
    org,
    postal,
    timezone
FROM domain_data.finalboss f;
"""

# Connect to DB and execute query
connection = mysql.connector.connect(**DB_CONFIG)
df = pd.read_sql(query, connection)

# Save to CSV
df.to_csv("distinct_IP.csv", index=False)

# Close connection
connection.close()

print("✅ Data exported to domain_data_export.csv")
