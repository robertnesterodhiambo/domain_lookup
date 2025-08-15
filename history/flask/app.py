from flask import Flask, render_template, request, send_file, jsonify, Response
import pandas as pd
import io
import pymysql
import os
import uuid
import html

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

# ---------- AUTOCOMPLETE SETUP ----------
SUGGESTION_FIELDS = {
    'domain_registrar_name': 'domain_registrar_name.xlsx',
    'registrant_name': 'registrant_name.xlsx',
    'registrant_company': 'registrant_company.xlsx',
    'registrant_address': 'registrant_address.xlsx',
    'registrant_city': 'registrant_city.xlsx',
    'registrant_state': 'registrant_state.xlsx',
    'registrant_zip': 'registrant_zip.xlsx',
    'registrant_country': 'registrant_country.xlsx',
}

suggestions_data = {}

def load_suggestions():
    """Load Excel data into memory for autocomplete."""
    data_folder = os.path.join(os.path.dirname(__file__), 'data')
    for field, filename in SUGGESTION_FIELDS.items():
        filepath = os.path.join(data_folder, filename)
        if os.path.exists(filepath):
            df = pd.read_excel(filepath, header=None)
            values = sorted(set(df.iloc[:, 0].dropna().astype(str)))
            suggestions_data[field] = values
        else:
            suggestions_data[field] = []

load_suggestions()

@app.route('/suggest/<field>')
def suggest(field):
    """Return JSON suggestions for the given field."""
    query = request.args.get('q', '').strip().lower()
    if field not in suggestions_data:
        return jsonify([])

    matches = []
    if query:
        matches = [v for v in suggestions_data[field] if query in v.lower()]

    return jsonify(matches[:20])  # Limit to 20 matches

# ---------- ROUTES ----------
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
            query += f" OR {key} LIKE %s"
            params.append(f"%{value}%")

    # Generate HTML progressively
    def generate_table():
        conn = get_db_connection()
        chunks = pd.read_sql(query, conn, params=params, chunksize=1000)

        first_chunk = True
        row_count = 0

        yield "<table class='table table-striped'><thead><tr>"

        for chunk in chunks:
            if first_chunk:
                # Send headers
                for col in chunk.columns:
                    yield f"<th>{html.escape(str(col))}</th>"
                yield "</tr></thead><tbody>"
                first_chunk = False

            # Send rows
            for _, row in chunk.iterrows():
                yield "<tr>" + "".join(f"<td>{html.escape(str(val))}</td>" for val in row) + "</tr>"
                row_count += 1

        conn.close()
        yield "</tbody></table>"

    # We still need to store full DataFrame for download (will re-read fully in background)
    conn = get_db_connection()
    full_df = pd.read_sql(query, conn, params=params)
    conn.close()

    token = str(uuid.uuid4())
    query_cache[token] = full_df

    # Render the page with streamed table
    table_html = "".join(generate_table())  # For now, collect generator output for preview.html
    return render_template(
        'preview.html',
        count=len(full_df),
        token=token,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
        preview_table=table_html
    )

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
    app.run(host='0.0.0.0', port=5050, debug=True)
