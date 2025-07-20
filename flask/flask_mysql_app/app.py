from flask import Flask, render_template
import mysql.connector
from db_config import DB_CONFIG

app = Flask(__name__)

@app.route('/')
def index():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM subdomain")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('index.html', records=records)

if __name__ == '__main__':
    app.run(debug=True)
