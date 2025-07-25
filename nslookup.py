#!/usr/bin/env python3

import subprocess
import pandas as pd
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import re
import csv  # <-- Import csv for quoting constants

INPUT_FILE = 'domain_count.csv'
OUTPUT_FILE = 'nslookup.csv'
THREADS = 99
LOCK = threading.Lock()
CHUNK_SIZE = 50000

NS_TYPES = ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'TXT', 'SRV', 'SOA']
counter = 0  # global progress tracker

def get_processed_domains():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['domain'])
        return set(df['domain'].astype(str).str.strip())
    except Exception:
        return set()

def skip_address(addr):
    addr = addr.strip()
    return addr.startswith('127.0.0.53') or addr.startswith('127.0.0.1') or addr == '127.0.0.53'

def parse_nslookup_output(qtype, output):
    lines = output.splitlines()
    results = []

    if qtype in ['A', 'AAAA']:
        for line in lines:
            match = re.search(r'Address:\s*(.+)', line)
            if match:
                addr = match.group(1).strip()
                if not skip_address(addr):
                    results.append(addr)

    elif qtype == 'CNAME':
        for line in lines:
            match = re.search(r'canonical name = (.+)', line)
            if match:
                results.append(match.group(1).strip())

    elif qtype == 'MX':
        for line in lines:
            match = re.search(r'mail exchanger = (.+)', line)
            if match:
                results.append(match.group(1).strip())

    elif qtype == 'NS':
        for line in lines:
            match = re.search(r'nameserver = (.+)', line)
            if match:
                results.append(match.group(1).strip())

    elif qtype == 'TXT':
        for line in lines:
            match = re.search(r'text = "(.+)"', line)
            if match:
                results.append(match.group(1).strip())

    elif qtype == 'SRV':
        for line in lines:
            match = re.search(r'mail addr = (.+)', line)
            if match:
                results.append(match.group(1).strip())

    elif qtype == 'SOA':
        for line in lines:
            match = re.search(r'serial = (.+)', line)
            if match:
                results.append(match.group(1).strip())

    return ' | '.join(results) if results else ''

def run_nslookups(domain):
    result = {}
    a_addresses = []

    for qtype in NS_TYPES:
        try:
            lookup = subprocess.run(
                ['nslookup', '-q=' + qtype, domain],
                capture_output=True,
                text=True,
                timeout=8
            )
            parsed = parse_nslookup_output(qtype, lookup.stdout)
            result[f'nslookup{qtype}'] = parsed

            if qtype == 'A' and parsed:
                a_addresses = [addr for addr in parsed.split(' | ') if not skip_address(addr)]

        except Exception as e:
            result[f'nslookup{qtype}'] = f"Error: {e}"

    ptr_results = []
    for address in a_addresses:
        try:
            lookup = subprocess.run(
                ['nslookup', address],
                capture_output=True,
                text=True,
                timeout=8
            )
            lines = lookup.stdout.splitlines()
            for line in lines:
                match_name = re.search(r'Name:\s*(.+)', line)
                if match_name:
                    name = match_name.group(1).strip()
                    if not skip_address(name):
                        ptr_results.append(name)
        except Exception as e:
            ptr_results.append(f"Error: {e}")

    result['nslookupPTR'] = ' | '.join(ptr_results) if ptr_results else ''

    return result

def run_dmarc_lookup(domain):
    dmarc_domain = f"_dmarc.{domain}"
    try:
        lookup = subprocess.run(
            ['nslookup', '-q=TXT', dmarc_domain],
            capture_output=True,
            text=True,
            timeout=8
        )
        lines = lookup.stdout.splitlines()
        results = []
        for line in lines:
            if '\t' in line and 'text = "' in line:
                parts = line.split('text = "')
                if len(parts) > 1:
                    text_value = parts[1].rstrip('"').strip()
                    results.append(text_value)
        return ' | '.join(results) if results else ''
    except Exception as e:
        return f"Error: {e}"

def save_result(row_data, all_columns):
    with LOCK:
        file_exists = os.path.exists(OUTPUT_FILE)
        df = pd.DataFrame([row_data], columns=all_columns)
        df.to_csv(
            OUTPUT_FILE,
            mode='a',
            index=False,
            header=not file_exists,
            quoting=csv.QUOTE_ALL   # <-- This quotes all fields, protecting commas and special chars inside
        )

        global counter
        counter += 1
        print(f"{counter} saved and processed")

def process_row(row, all_columns):
    domain = str(row['domain']).strip()
    ns_data = run_nslookups(domain)
    dmarc_data = run_dmarc_lookup(domain)
    ns_data['nslookuptxt_dmarc'] = dmarc_data

    full_data = dict(row)
    full_data.update(ns_data)
    save_result(full_data, all_columns)

def process_chunk(chunk, processed_domains, all_columns):
    chunk['domain'] = chunk['domain'].astype(str).str.strip()
    remaining = chunk[~chunk['domain'].isin(processed_domains)]

    if remaining.empty:
        print("All domains in this chunk are already processed. Skipping chunk.")
        return

    exact_nl = remaining[remaining['domain'].str.match(r'^[^.]+\.(nl)$', na=False)]
    sub_nl = remaining[remaining['domain'].str.match(r'^.+\.(.+\.)?nl$', na=False) & ~remaining['domain'].str.match(r'^[^.]+\.(nl)$', na=False)]
    others = remaining[~remaining.index.isin(exact_nl.index) & ~remaining.index.isin(sub_nl.index)]
    prioritized = pd.concat([exact_nl, sub_nl, others], ignore_index=True)

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for _, row in prioritized.iterrows():
            executor.submit(process_row, row, all_columns)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    processed_domains = get_processed_domains()
    print(f"🟡 Already processed: {len(processed_domains)}")

    for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE):
        print(f"🔹 Processing new chunk of size: {len(chunk)}")
        base_columns = chunk.columns.tolist()
        ns_columns = [f'nslookup{q}' for q in NS_TYPES] + ['nslookupPTR', 'nslookuptxt_dmarc']
        all_columns = base_columns + ns_columns

        process_chunk(chunk, processed_domains, all_columns)

if __name__ == '__main__':
    main()
