from flask import Flask, request, render_template, Response, stream_with_context
import mysql.connector
from mysql.connector import connect

app = Flask(__name__)

DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

# HTML form page
@app.route('/')
def index():
    return render_template('index.html')

# Streaming endpoint
@app.route('/stream', methods=['GET'])
def stream_data():
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

        # Send headers first (for CSV example)
        yield ",".join(cursor.column_names) + "\n"

        for row in cursor:
            yield ",".join(str(row[col]) if row[col] is not None else '' for col in cursor.column_names) + "\n"

        cursor.close()
        conn.close()

    return Response(stream_with_context(generate()), mimetype='text/csv')

if __name__ == '__main__':
    app.run(debug=True, threaded=True)

