from flask import Flask, render_template, request
import mysql.connector
from db_config import DB_CONFIG

app = Flask(__name__)

def get_unique_values(column):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = f"SELECT DISTINCT {column} FROM nslookup WHERE {column} IS NOT NULL AND {column} != ''"
    cursor.execute(query)
    results = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return sorted(results)

@app.route('/', methods=['GET', 'POST'])
def index():
    records = []
    filters = {
        'tld': request.form.get('tld'),
        'registrar': request.form.get('registrar'),
        'country': request.form.get('country'),
        'start_date': request.form.get('start_date'),
        'end_date': request.form.get('end_date')
    }

    if request.method == 'POST':
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        conditions = []
        values = []

        if filters['tld']:
            conditions.append("tld = %s")
            values.append(filters['tld'])
        if filters['registrar']:
            conditions.append("registrar = %s")
            values.append(filters['registrar'])
        if filters['country']:
            conditions.append("registrant_country = %s")
            values.append(filters['country'])
        if filters['start_date']:
            conditions.append("lookup_date >= %s")
            values.append(filters['start_date'])
        if filters['end_date']:
            conditions.append("lookup_date <= %s")
            values.append(filters['end_date'])

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        cursor.execute(f"SELECT * FROM accessibility {where_clause}", values)
        records = cursor.fetchall()
        cursor.close()
        conn.close()

    tlds = get_unique_values('tld')
    registrars = get_unique_values('registrar')
    countries = get_unique_values('registrant_country')

    return render_template(
        'index.html',
        records=records,
        tlds=tlds,
        registrars=registrars,
        countries=countries,
        filters=filters
    )

if __name__ == '__main__':
    app.run(debug=True)
