import requests
from bs4 import BeautifulSoup
import csv

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

# Mapping from possible WHOIS keys to CSV column names
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
    "Nameserver": "Nameserver"  # will handle multiple separately
}

def fetch_whois(domain_url):
    headers = {"User-Agent": "curl/7.68.0"}
    response = requests.get(domain_url, headers=headers)
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
                # Map to fixed column names if exists
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
    urls = [
        "https://www.whois.com/whois/007.lt"
        # Add more WHOIS URLs if needed
    ]
    
    all_data = []
    for url in urls:
        data = fetch_whois(url)
        if data:
            all_data.append(data)
    
    if all_data:
        save_to_csv(all_data)
        print(f"WHOIS data saved to {OUTPUT_CSV}")
