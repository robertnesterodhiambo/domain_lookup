from flask import Flask, render_template, request, send_file, redirect, url_for
import pandas as pd
import io
import pymysql
from datetime import datetime
import uuid

app = Flask(__name__)
app.secret_key = '556'  # Required for session

# DB config
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

# Store results in-memory (simple cache for this use case)
query_cache = {}

def get_db_connection():
    return pymysql.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database']
    )

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/preview', methods=['GET'])
def preview():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    limit = request.args.get('limit')

    filters = {
        'domain_registrar_name': request.args.get('domain_registrar_name', ''),
        'registrant_name': request.args.get('registrant_name', ''),
        'registrant_company': request.args.get('registrant_company', ''),
        'registrant_address': request.args.get('registrant_address', ''),
        'registrant_city': request.args.get('registrant_city', ''),
        'registrant_state': request.args.get('registrant_state', ''),
        'registrant_zip': request.args.get('registrant_zip', ''),
        'registrant_country': request.args.get('registrant_country', ''),
    }

    query = "SELECT * FROM group_1 WHERE DATE(create_date) BETWEEN %s AND %s"
    params = [start_date, end_date]

    for key, value in filters.items():
        if value:
            query += f" AND {key} LIKE %s"
            params.append(f"%{value}%")

    if limit == "1000":
        query += " LIMIT 1000"

    conn = get_db_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()

    # Store result in memory with a token
    token = str(uuid.uuid4())
    query_cache[token] = df

    return render_template('preview.html', count=len(df), token=token, limit=limit, start_date=start_date, end_date=end_date)

@app.route('/download/<token>')
def download(token):
    df = query_cache.get(token)
    if df is None:
        return "Session expired or invalid download token", 404

    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    filename = f"filtered_data_{token}.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        download_name=filename,
        as_attachment=True,
        mimetype='text/csv'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)

