from flask import Flask, render_template, request, send_file
import mysql.connector
from db_config import DB_CONFIG
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_unique_values(column):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = f"SELECT DISTINCT {column} FROM nslookup WHERE {column} IS NOT NULL AND {column} != ''"
    cursor.execute(query)
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return sorted(results)

@app.route('/', methods=['GET'])
def index():
    tlds = get_unique_values('tld')
    registrars = get_unique_values('registrar')
    countries = get_unique_values('registrant_country')
    return render_template(
        'index.html',
        tlds=tlds,
        registrars=registrars,
        countries=countries
    )

@app.route('/download', methods=['POST'])
def download():
    tld = request.form.get('tld')
    registrar = request.form.get('registrar')
    country = request.form.get('country')

    print("Download requested with filters:", tld, registrar, country)

    conditions = []
    values = []

    if tld:
        conditions.append("tld = %s")
        values.append(tld)
    if registrar:
        conditions.append("registrar = %s")
        values.append(registrar)
    if country:
        conditions.append("registrant_country = %s")
        values.append(country)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"SELECT * FROM nslookup {where_clause}"
    print("Query:", query)
    print("Values:", values)

    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn, params=values)
    conn.close()

    if df.empty:
        return "No data matched the filters selected."

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"filtered_nslookup_{timestamp}.xlsx"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    df.to_excel(filepath, index=False)

    return send_file(filepath, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
