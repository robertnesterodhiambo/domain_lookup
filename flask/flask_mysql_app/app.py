from flask import Flask, render_template, request
import mysql.connector
from db_config import DB_CONFIG

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Get unique filter values
    cursor.execute("SELECT DISTINCT domain FROM accessibility ORDER BY domain")
    unique_domains = [row['domain'] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT tld FROM accessibility ORDER BY tld")
    unique_tlds = [row['tld'] for row in cursor.fetchall()]

    cursor.execute("SELECT MIN(lookup_date) as min_date, MAX(lookup_date) as max_date FROM accessibility")
    date_range = cursor.fetchone()
    min_date = str(date_range['min_date']) if date_range['min_date'] else ''
    max_date = str(date_range['max_date']) if date_range['max_date'] else ''

    # Filter inputs
    domain = request.args.get('domain')
    tld = request.args.get('tld')
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')

    records = []
    if domain or tld or date_start or date_end:
        filters = []
        params = []

        if domain:
            filters.append("domain = %s")
            params.append(domain)
        if tld:
            filters.append("tld = %s")
            params.append(tld)
        if date_start:
            filters.append("lookup_date >= %s")
            params.append(date_start)
        if date_end:
            filters.append("lookup_date <= %s")
            params.append(date_end)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"SELECT * FROM accessibility {where_clause} LIMIT 1000"

        cursor.execute(query, params)
        records = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template(
        'index.html',
        records=records,
        unique_domains=unique_domains,
        unique_tlds=unique_tlds,
        domain=domain,
        tld=tld,
        date_start=date_start or min_date,
        date_end=date_end or max_date
    )

if __name__ == '__main__':
    app.run(debug=True)
