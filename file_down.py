import mysql.connector
import pandas as pd

DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165'
}

# Connect to MySQL server
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

# Fetch data from domain_data.finalboss
cursor.execute("SELECT domain, nslookupA AS nslookup FROM domain_data.finalboss")
finalboss_data = cursor.fetchall()

# Fetch data from complete.complete
cursor.execute("SELECT domain, nslookupa AS nslookup FROM complete.complete")
complete_data = cursor.fetchall()

cursor.close()
conn.close()

# Convert to DataFrames
df_finalboss = pd.DataFrame(finalboss_data)
df_complete = pd.DataFrame(complete_data)

# Combine both DataFrames (stack vertically)
df_combined = pd.concat([df_finalboss, df_complete], ignore_index=True)

# Export to CSV
df_combined.to_csv('combined_nslookup.csv', index=False)

print("Data exported successfully to combined_nslookup.csv")

