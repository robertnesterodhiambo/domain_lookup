import mysql.connector
import pandas as pd
from openpyxl import Workbook

# Your DB config
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

# List of columns
columns = [
    'domain_registrar_name',    'registrant_name', 'registrant_company', 'registrant_address', 'registrant_city', 'registrant_state',
    'registrant_zip', 'registrant_country'
]

# Connect to MySQL
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

for col in columns:
    try:
        print(f"Fetching unique values for column: {col}")
        cursor.execute(f"SELECT DISTINCT `{col}` FROM `group_1` LIMIT 10000")
        rows = cursor.fetchall()
        values = [row[0] for row in rows]

        # Create a DataFrame
        df = pd.DataFrame(values, columns=[col])

        # Save to a compact Excel file
        filename = f"{col}.xlsx"
        df.to_excel(filename, index=False, engine='openpyxl')

    except Exception as e:
        print(f"Error processing {col}: {e}")

cursor.close()
conn.close()
print("✅ Done. All Excel files created.")
