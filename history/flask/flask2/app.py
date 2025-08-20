from flask import Flask, request, render_template, jsonify, Response, stream_with_context
from mysql.connector import connect

app = Flask(__name__)

DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

TABLE_NAME = 'group_1'

# Fixed allowed columns (from your list)
ALLOWED_COLUMNS = [
    "num",
    "domain_name",
    "query_time",
    "create_date",
    "update_date",
    "expiry_date",
    "domain_registrar_id",
    "domain_registrar_name",
    "domain_registrar_whois",
    "domain_registrar_url",
    "registrant_name",
    "registrant_company",
    "registrant_address",
    "registrant_city",
    "registrant_state",
    "registrant_zip",
    "registrant_country",
    "registrant_email",
    "registrant_phone",
    "registrant_fax",
    "administrative_name",
    "administrative_company",
    "administrative_address",
    "administrative_city",
    "administrative_state",
    "administrative_zip",
    "administrative_country",
    "administrative_email",
    "administrative_phone",
    "administrative_fax",
    "technical_name",
    "technical_company",
    "technical_address",
    "technical_city",
    "technical_state",
    "technical_zip",
    "technical_country",
    "technical_email",
    "technical_phone",
    "technical_fax",
    "billing_name",
    "billing_company",
    "billing_address",
    "billing_city",
    "billing_state",
    "billing_zip",
    "billing_country",
    "billing_email",
    "billing_phone",
    "billing_fax",
    "name_server_1",
    "name_server_2",
    "name_server_3",
    "name_server_4",
    "domain_status_1",
    "domain_status_2",
    "domain_status_3",
    "domain_status_4",
]

def normalize_columns_param(columns_param):
    """
    Turn comma-separated 'columns' into a safe, ordered list based on ALLOWED_COLUMNS.
    If empty/invalid, default to all allowed columns.
    """
    if not columns_param:
        return ALLOWED_COLUMNS[:]
    requested = [c.strip() for c in columns_param.split(",") if c.strip()]
    safe = [c for c in requested if c in ALLOWED_COLUMNS]
    return safe if safe else ALLOWED_COLUMNS[:]

def build_select_list(columns):
    """
    Backtick-quote column names for SQL.
    """
    return ", ".join(f"`{c}`" for c in columns)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/columns')
def list_columns():
    # Frontend will populate the multi-select from here
    return jsonify(ALLOWED_COLUMNS)

@app.route('/data')
def get_data():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    columns_param = request.args.get('columns')  # comma-separated list (optional)
    page = int(request.args.get('page', 1))
    per_page = 50
    offset = (page - 1) * per_page

    if not start_date or not end_date:
        return jsonify({"error": "Missing date range"}), 400

    selected_cols = normalize_columns_param(columns_param)
    select_list = build_select_list(selected_cols)

    conn = connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"""
            SELECT {select_list}
            FROM `{TABLE_NAME}`
            WHERE `create_date` BETWEEN %s AND %s
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (start_date, end_date, per_page, offset))
        rows = cursor.fetchall()
        return jsonify(rows)
    finally:
        cursor.close()
        conn.close()

@app.route('/download')
def download_csv():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    columns_param = request.args.get('columns')

    if not start_date or not end_date:
        return "Missing date range", 400

    selected_cols = normalize_columns_param(columns_param)
    select_list = build_select_list(selected_cols)

    def generate():
        conn = connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        try:
            query = f"""
                SELECT {select_list}
                FROM `{TABLE_NAME}`
                WHERE `create_date` BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))

            # Header
            yield ",".join(cursor.column_names) + "\n"

            # Rows
            for row in cursor:
                yield ",".join(
                    ("" if row[col] is None else str(row[col]))
                    for col in cursor.column_names
                ) + "\n"
        finally:
            cursor.close()
            conn.close()

    return Response(
        stream_with_context(generate()),
        mimetype='text/csv',
        headers={"Content-Disposition": "attachment; filename=data.csv"}
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True, threaded=True)
