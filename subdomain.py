import csv
import subprocess
import os

INPUT_FILE = 'whois_results.csv'
OUTPUT_FILE = 'db_excel.csv'

def count_subdomains(domain):
    try:
        result = subprocess.run(
            ['subfinder', '-d', domain, '-silent'],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=True
        )
        return len(result.stdout.strip().splitlines())
    except subprocess.CalledProcessError:
        return 0

# Step 1: Load already processed domains
processed_domains = set()
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed_domains.add(row['domain'].strip())

# Step 2: Start appending new results
write_header = not os.path.exists(OUTPUT_FILE)

with open(INPUT_FILE, newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['subdomain_count']

    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
            outfile.flush()

        for row in reader:
            domain = row.get('domain', '').strip()
            if not domain:
                print("⚠️ Skipping row with no domain.")
                continue

            if domain in processed_domains:
                print(f"⏩ Skipping already processed domain: {domain}")
                continue

            sub_count = count_subdomains(domain)
            row['subdomain_count'] = sub_count
            writer.writerow(row)
            outfile.flush()
            processed_domains.add(domain)  # Mark as done
            print(f"✅ {domain}: {sub_count} subdomains written.")
