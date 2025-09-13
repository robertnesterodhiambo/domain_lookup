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
    # ✅ Connect to main DB for finalboss
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM finalboss")
    finalboss_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    # ✅ Connect to complete DB for complete table
    complete_db_config = DB_CONFIG.copy()
    complete_db_config['database'] = 'complete'
    conn2 = mysql.connector.connect(**complete_db_config)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT COUNT(*) FROM complete")
    complete_count = cursor2.fetchone()[0]
    cursor2.close()
    conn2.close()

    # ✅ total_collected = sum of both
    total_collected = finalboss_count + complete_count

    # ✅ Hard-coded total_possible
    total_possible = 337113538

    percent_collected = round((total_collected / total_possible) * 100, 2)

    # Get values from finalboss
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

    # Step 1: check if TLD exists in finalboss
    conn_check = mysql.connector.connect(**DB_CONFIG)
    cursor_check = conn_check.cursor()
    cursor_check.execute("SELECT 1 FROM finalboss WHERE tld = %s LIMIT 1", (tld,))
    exists_in_finalboss = cursor_check.fetchone()
    cursor_check.close()
    conn_check.close()

    if exists_in_finalboss:
        # ✅ Query finalboss
        conn = mysql.connector.connect(**DB_CONFIG)
        query = f"SELECT * FROM finalboss {where_clause}"
        df = pd.read_sql(query, conn, params=values)

        if df.empty:
            conn.close()
            return "No data matched the filters selected."

        # Site count from complete (same DB)
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

    else:
        # ✅ Query from external complete DB (filters not applied)
        other_db_config = DB_CONFIG.copy()
        other_db_config['database'] = 'complete'
        conn = mysql.connector.connect(**other_db_config)
        query = "SELECT * FROM complete WHERE tld = %s"
        df = pd.read_sql(query, conn, params=[tld])

        if df.empty:
            conn.close()
            return "No data found for the selected TLD."

        # Site count from this complete DB
        count_query = """
            SELECT tld, COUNT(DISTINCT domain) AS site_count
            FROM complete
            WHERE tld = %s
            GROUP BY tld
        """
        cursor = conn.cursor()
        cursor.execute(count_query, [tld])
        count_results = cursor.fetchall()
        cursor.close()
        conn.close()

    # Add site_count column
    tld_counts = {row[0]: row[1] for row in count_results}
    df['site_count'] = df['tld'].map(tld_counts).fillna(0).astype(int)

    # Save file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"filtered_nslookup_{timestamp}.xlsx"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    df.to_excel(filepath, index=False)

    return send_file(filepath, as_attachment=True)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
