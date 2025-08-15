from flask import Flask, request, render_template, jsonify, Response, stream_with_context
from mysql.connector import connect

app = Flask(__name__)

DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    page = int(request.args.get('page', 1))
    per_page = 50
    offset = (page - 1) * per_page

    if not start_date or not end_date:
        return jsonify({"error": "Missing date range"}), 400

    conn = connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT * FROM group_1
    WHERE create_date BETWEEN %s AND %s
    LIMIT %s OFFSET %s
    """
    cursor.execute(query, (start_date, end_date, per_page, offset))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(rows)

@app.route('/download')
def download_csv():
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    if not start_date or not end_date:
        return "Missing date range", 400

    def generate():
        conn = connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT * FROM group_1
        WHERE create_date BETWEEN %s AND %s
        """
        cursor.execute(query, (start_date, end_date))

        yield ",".join(cursor.column_names) + "\n"
        for row in cursor:
            yield ",".join(str(row[col]) if row[col] is not None else '' for col in cursor.column_names) + "\n"

        cursor.close()
        conn.close()

    return Response(stream_with_context(generate()), mimetype='text/csv',
                    headers={"Content-Disposition": "attachment; filename=data.csv"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True, threaded=True)

