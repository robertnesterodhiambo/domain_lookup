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
    query = f"SELECT DISTINCT {column} FROM finalboss WHERE {column} IS NOT NULL AND {column} != ''"
    cursor.execute(query)
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return sorted(results)

@app.route('/', methods=['GET'])
def index():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM finalboss")
    total_collected = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(count) FROM complete")
    total_possible = cursor.fetchone()[0] or 1

    percent_collected = round((total_collected / total_possible) * 100, 2)

    cursor.close()
    conn.close()

    tlds = get_unique_values('tld')
    registrars = get_unique_values('registrar_name')
    countries = get_unique_values('registrar_country')

    return render_template(
        'index.html',
        tlds=tlds,
        registrars=registrars,
        countries=countries,
        total_collected=total_collected,
        total_possible=total_possible,
        percent_collected=percent_collected
    )

@app.route('/preview', methods=['POST'])
def preview():
    tld = request.form.get('tld')
    registrar = request.form.get('registrar_name')
    country = request.form.get('country')

    conditions = []
    values = []

    if tld:
        conditions.append("tld = %s")
        values.append(tld)
    if registrar:
        conditions.append("registrar_name = %s")
        values.append(registrar)
    if country:
        conditions.append("registrar_country = %s")
        values.append(country)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT COUNT(*) FROM finalboss {where_clause}"

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query, values)
    filtered_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return render_template(
        'preview.html',
        filtered_count=filtered_count,
        tld=tld,
        registrar=registrar,
        country=country
    )

@app.route('/download', methods=['POST'])
def download():
    tld = request.form.get('tld')
    registrar = request.form.get('registrar_name')
    country = request.form.get('country')

    conditions = []
    values = []

    if tld:
        conditions.append("tld = %s")
        values.append(tld)
    if registrar:
        conditions.append("registrar_name = %s")
        values.append(registrar)
    if country:
        conditions.append("registrar_country = %s")
        values.append(country)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM finalboss {where_clause}"

    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn, params=values)

    if df.empty:
        conn.close()
        return "No data matched the filters selected."

    selected_count = len(df)
    print(f"User selected {selected_count} rows.")

    count_query = f"""
        SELECT tld, COUNT(DISTINCT domain) AS site_count
        FROM complete
        {where_clause}
        GROUP BY tld
    """
    cursor = conn.cursor()
    cursor.execute(count_query, values)
    count_results = cursor.fetchall()
    cursor.close()
    conn.close()

    tld_counts = {row[0]: row[1] for row in count_results}
    df['site_count'] = df['tld'].map(tld_counts).fillna(0).astype(int)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"filtered_nslookup_{timestamp}.xlsx"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    df.to_excel(filepath, index=False)

    return send_file(filepath, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
