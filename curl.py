#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd

INPUT_CSV = "data_rdap.csv"
OUTPUT_CSV = "data_rdap_parsed.csv"

# Fixed CSV columns
CSV_COLUMNS = [
    "Domain",
    "Status",
    "Registered",
    "Expires",
    "Registrar",
    "Registrar website",
    "Registrar email",
    "Contact organization",
    "Contact email",
    "Nameserver1",
    "Nameserver2",
    "Nameserver3",
    "Nameserver4"
]

# Mapping from WHOIS keys to CSV column names
KEY_MAPPING = {
    "Domain": "Domain",
    "Status": "Status",
    "Registered": "Registered",
    "Expires": "Expires",
    "Registrar": "Registrar",
    "Registrar website": "Registrar website",
    "Registrar email": "Registrar email",
    "Contact organization": "Contact organization",
    "Contact email": "Contact email",
    "Nameserver": "Nameserver"  # handled separately
}

# Proxy config
username = "geonode_DrXb2XNsHm-type-residential"
password = "f232262f-0f34-400c-a7a6-84d1ce423302"
GEONODE_DNS = "92.204.164.15:9000"

PROXIES = {
    "http": f"http://{username}:{password}@{GEONODE_DNS}",
    "https": f"http://{username}:{password}@{GEONODE_DNS}"
}

def fetch_whois(domain_url):
    headers = {"User-Agent": "curl/7.68.0"}
    try:
        response = requests.get(domain_url, headers=headers, proxies=PROXIES, timeout=20)
    except Exception as e:
        print(f"Request failed for {domain_url}: {e}")
        return None

    if response.status_code != 200:
        print(f"Bad response {response.status_code} for {domain_url}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    whois_block = soup.find("pre", id="registryData")
    
    if not whois_block:
        print(f"WHOIS data not found for {domain_url}")
        return None
    
    lines = whois_block.get_text().splitlines()
    
    data_dict = {}
    nameservers = []

    for line in lines:
        line = line.strip()
        if not line or line == "%":
            continue
        
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            
            if key.lower().startswith("nameserver"):
                nameservers.append(value)
            else:
                csv_key = KEY_MAPPING.get(key)
                if csv_key:
                    data_dict[csv_key] = value
    
    # Add up to 4 nameservers
    for i in range(4):
        col_name = f"Nameserver{i+1}"
        data_dict[col_name] = nameservers[i] if i < len(nameservers) else "N/A"
    
    # Ensure all fixed columns exist
    for col in CSV_COLUMNS:
        if col not in data_dict:
            data_dict[col] = "N/A"
    
    return data_dict

def save_to_csv(data_list, filename=OUTPUT_CSV):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for data in data_list:
            writer.writerow(data)

if __name__ == "__main__":
    # Load domains from input CSV
    df = pd.read_csv(INPUT_CSV)

    # Normalize headers (strip spaces, lowercase)
    df.columns = df.columns.str.strip().str.lower()
    if "domain" not in df.columns:
        raise ValueError("Input CSV must have a 'Domain' column")

    domains = df["domain"].head(10).tolist()  # first 10 rows only
    
    all_data = []
    for domain in domains:
        url = f"https://www.whois.com/whois/{domain}"
        print(f"Fetching WHOIS for {domain}...")
        data = fetch_whois(url)
        if data:
            data["Domain"] = domain  # make sure Domain field is filled
            all_data.append(data)
            print("Extracted data:", data)
    
    if all_data:
        save_to_csv(all_data)
        print(f"WHOIS data saved to {OUTPUT_CSV}")
