import subprocess
import json
import pandas as pd

FIELDS = [
    "Apache", "Country", "HTTPServer", "IP", "RedirectLocation", "Title",
    "Bootstrap", "Email", "JQuery", "Meta-Author", "Open-Graph-Protocol",
    "PoweredBy", "Script", "Strict-Transport-Security", "UncommonHeaders",
    "WordPress", "X-Frame-Options"
]

def scan_domain(domain):
    result = subprocess.run(
        ["whatweb", "--log-json=-", domain],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    records = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line in ["[", "]", ","]:
            continue
        try:
            data = json.loads(line)
            record = {"Target": data.get("target")}
            plugins = data.get("plugins", {})
            for field in FIELDS:
                value = None
                if field in plugins:
                    if "string" in plugins[field]:
                        value = plugins[field]["string"]
                    elif "version" in plugins[field]:
                        value = plugins[field]["version"]
                record[field] = ", ".join(value) if value else None
            records.append(record)
        except json.JSONDecodeError:
            continue
    return records

def pick_most_data(records):
    # pick the row with the most filled fields
    return max(records, key=lambda r: sum(1 for v in r.values() if v and v != "None"))

if __name__ == "__main__":
    domain = "spixnet.ai"
    records = scan_domain(domain)
    if records:
        best_record = pick_most_data(records)
        df = pd.DataFrame([best_record])
        df.to_csv("whatweb_results.csv", index=False)
        print(f"Saved best result to whatweb_results.csv")
    else:
        print("No records found.")
