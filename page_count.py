import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_CSV = 'nslookup.csv'
OUTPUT_CSV = 'page_count.csv'
MAX_THREADS = 200  # Adjust based on your machine/network capacity
CHUNK_SIZE = 5000
PRINT_TO_TERMINAL = False  # Set True to print output rows to terminal
PRINT_CMD_OUTPUT = True    # Set True to print wget command output

def count_pages(domain):
    url = f'https://{domain}'
    try:
        cmd = (
            f"wget --spider --recursive --level=5 --no-verbose --no-directories "
            f"--no-hsts --timeout=10 --tries=2 '{url}' 2>&1 | "
            f"grep -o '{url}[^ ]*' | sort -u | wc -l"
        )
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
            timeout=60  # timeout in seconds for each subprocess
        )
        if PRINT_CMD_OUTPUT:
            print(f"Command output for {domain}:\n{result.stdout.strip()}\n")
        page_count = int(result.stdout.strip())
    except subprocess.TimeoutExpired:
        print(f"Timeout expired for {domain}")
        page_count = 0
    except Exception as e:
        print(f'Error for {domain}: {e}')
        page_count = 0

    return domain, page_count

def is_processed(row):
    val = row.get('pages_count')
    if val is None:
        return False
    val = val.strip()
    if not val:
        return False
    try:
        int(val)
        return True
    except ValueError:
        return False

def load_processed_domains(output_file):
    """
    Loads domains already processed (with pages_count) from the output CSV
    Returns a dict mapping domain -> row (with pages_count)
    """
    processed = {}
    if not os.path.exists(output_file):
        return processed

    with open(output_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = row.get('domain')
            if domain and is_processed(row):
                processed[domain] = row
    print(f"Loaded {len(processed)} processed domains from {output_file}")
    return processed

def process_chunk(rows, fieldnames, writer, outfile, processed_domains):
    skipped_count = 0
    to_process_rows = []

    for row in rows:
        domain = row['domain']
        if domain in processed_domains:
            # Use already processed row from processed_domains
            writer.writerow(processed_domains[domain])
            skipped_count += 1
            if PRINT_TO_TERMINAL:
                print(processed_domains[domain])
            outfile.flush()
            os.fsync(outfile.fileno())
        else:
            to_process_rows.append(row)

    print(f"Chunk info: {len(rows)} rows total | Skipping {skipped_count} already processed | Processing {len(to_process_rows)} rows now")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(count_pages, row['domain']): row for row in to_process_rows}

        for future in as_completed(futures):
            row = futures[future]
            domain, count = future.result()
            row['pages_count'] = count
            # Write newly processed row and flush immediately
            writer.writerow(row)
            outfile.flush()
            os.fsync(outfile.fileno())
            if PRINT_TO_TERMINAL:
                print(row)
            print(f'{domain} → {count} pages')

def main():
    # Load already processed domains from output CSV if it exists
    processed_domains = load_processed_domains(OUTPUT_CSV)

    with open(INPUT_CSV, newline='') as infile, open(OUTPUT_CSV, 'a', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        if 'pages_count' not in fieldnames:
            fieldnames += ['pages_count']

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        # Write header only if output file is empty
        if outfile.tell() == 0:
            writer.writeheader()
            outfile.flush()
            os.fsync(outfile.fileno())

        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                process_chunk(chunk, fieldnames, writer, outfile, processed_domains)
                chunk = []

        # Process remaining rows
        if chunk:
            process_chunk(chunk, fieldnames, writer, outfile, processed_domains)

if __name__ == '__main__':
    main()
