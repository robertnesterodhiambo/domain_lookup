from flask import Flask, render_template, request, send_file
import pandas as pd
import pymysql
import io

pymysql.install_as_MySQLdb()
import MySQLdb

app = Flask(__name__)

DB_CONFIG = {
    'user': 'root',
    'passwd': 'root235',
    'host': '46.62.140.165',
    'db': 'history'
}

def get_db_connection():
    return MySQLdb.connect(**DB_CONFIG)

@app.route('/', methods=['GET', 'POST'])
def index():
    start_date = end_date = ""
    download_ready = False

    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        download_ready = True  # show buttons immediately

    return render_template('index.html',
                           start_date=start_date,
                           end_date=end_date,
                           download_ready=download_ready)

@app.route('/download')
def download():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    limit = request.args.get('limit')

    if not start_date or not end_date:
        return "Missing date range."

    conn = get_db_connection()

    query = """
        SELECT * FROM group_1
        WHERE create_date BETWEEN %s AND %s
    """
    if limit == "1000":
        query += " LIMIT 1000"

    df = pd.read_sql(query, conn, params=(start_date, end_date))
    conn.close()

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    suffix = "limited_1000" if limit == "1000" else "full"
    filename = f"filtered_data_{suffix}_{start_date}_to_{end_date}.xlsx"

    return send_file(output, download_name=filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
