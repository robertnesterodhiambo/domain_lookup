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

def merge_records(records):
    if not records:
        return None
    
    # Pick the record with the most filled fields as base
    best_record = max(records, key=lambda r: sum(1 for v in r.values() if v and v != "None"))
    
    # Merge other records into it
    for rec in records:
        if rec is best_record:
            continue
        for k, v in rec.items():
            if not best_record.get(k) and v:  # fill only if empty
                best_record[k] = v
    return best_record

if __name__ == "__main__":
    domain = "spixnet.ai"
    records = scan_domain(domain)
    merged_record = merge_records(records)
    
    if merged_record:
        df = pd.DataFrame([merged_record])
        df.to_csv("whatweb_results.csv", index=False)
        print(f"Saved merged best result to whatweb_results.csv")
    else:
        print("No records found.")
