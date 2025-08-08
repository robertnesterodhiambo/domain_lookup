from flask import Flask, render_template, jsonify, request
import os
import pandas as pd

app = Flask(__name__)
DATA_FOLDER = 'data'

SELECTED_FIELDS = [
    'domain_registrar_name',
    'registrant_name',
    'registrant_company',
    'registrant_address',
    'registrant_city',
    'registrant_state',
    'registrant_zip',
    'registrant_country'
]

# Cache full data and preview (10 rows)
cached_data = {}
preview_data = {}

def load_selected_excel_files():
    global cached_data, preview_data
    print("📥 Importing Excel files...")
    for field in SELECTED_FIELDS:
        filename = f"{field}.xlsx"
        path = os.path.join(DATA_FOLDER, filename)
        if os.path.exists(path):
            try:
                df = pd.read_excel(path, engine='openpyxl', usecols=[0])
                values = df.iloc[:, 0].dropna().astype(str)
                cached_data[field] = values.unique().tolist()
                preview_data[field] = values.head(10).tolist()
                print(f"✅ Loaded: {filename} ({len(values)} total, showing 10)")
            except Exception as e:
                print(f"❌ Error loading {filename}: {e}")
        else:
            print(f"⚠️  File not found: {filename}")

@app.route("/")
def index():
    return render_template("index.html", fields=SELECTED_FIELDS, preview_data=preview_data)

@app.route("/api/options/<field>")
def get_options(field):
    query = request.args.get("q", "").lower()
    options = cached_data.get(field, [])
    filtered = [opt for opt in options if query in opt.lower()]
    return jsonify(filtered[:5])  # Limit to 5

if __name__ == "__main__":
    load_selected_excel_files()
    print("🚀 Flask running on http://0.0.0.0:5001")
    app.run(host="0.0.0.0", port=5001, debug=True)

