from flask import Flask, render_template
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

# Your database configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'root235',
    'host': '46.62.140.165',
    'database': 'history'
}

def test_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return True
    except Error as e:
        print(f"Connection error: {e}")
    return False

@app.route('/')
def home():
    if test_connection():
        message = "✅ Connected to database!"
    else:
        message = "❌ Failed to connect to database."
    return render_template('index.html', message=message)

if __name__ == '__main__':
    app.run(debug=True)
