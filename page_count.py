import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_CSV = 'tes_accesibilty.csv'
OUTPUT_CSV = 'tes_accesibilty_with_pages.csv'
MAX_THREADS = 99  # Adjust based on your machine/network capacity
CHUNK_SIZE = 5000
PRINT_TO_TERMINAL = False  # Set to True to print output rows to terminal
PRINT_CMD_OUTPUT = True    # New flag: print wget command output to terminal

def count_pages(domain):
    url = f'https://{domain}'
    try:
        cmd = (
            f"wget --spider --recursive --level=5 --no-verbose --no-directories --no-hsts '{url}' 2>&1 | "
            f"grep -o '{url}[^ ]*' | sort -u | wc -l"
        )
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        if PRINT_CMD_OUTPUT:
            print(f"Command output for {domain}:\n{result.stdout.strip()}\n")
        page_count = int(result.stdout.strip())
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

def process_chunk(rows, fieldnames, writer, outfile):
    skipped_count = sum(1 for row in rows if is_processed(row))
    to_process_count = len(rows) - skipped_count

    print(f"Chunk info: {len(rows)} rows total | Skipping {skipped_count} already processed | Processing {to_process_count} rows now")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}

        for row in rows:
            if is_processed(row):
                writer.writerow(row)
                writer.flush()
                outfile.flush()
                os.fsync(outfile.fileno())
                if PRINT_TO_TERMINAL:
                    print(row)
                continue
            futures[executor.submit(count_pages, row['domain'])] = row

        for future in as_completed(futures):
            row = futures[future]
            domain, count = future.result()
            row['pages_count'] = count
            writer.writerow(row)
            writer.flush()
            outfile.flush()
            os.fsync(outfile.fileno())
            if PRINT_TO_TERMINAL:
                print(row)
            print(f'{domain} → {count} pages')

def main():
    with open(INPUT_CSV, newline='') as infile, open(OUTPUT_CSV, 'a', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        if 'pages_count' not in fieldnames:
            fieldnames += ['pages_count']

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        if outfile.tell() == 0:
            writer.writeheader()
            writer.flush()
            outfile.flush()
            os.fsync(outfile.fileno())

        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                process_chunk(chunk, fieldnames, writer, outfile)
                chunk = []

        if chunk:
            process_chunk(chunk, fieldnames, writer, outfile)

if __name__ == '__main__':
    main()
