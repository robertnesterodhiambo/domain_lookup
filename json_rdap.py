import requests
import csv

def fetch_iana_rdap():
    url = 'https://data.iana.org/rdap/dns.json'
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def parse_and_save_rdap(data, output_csv):
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['tld', 'rdap'])  # header

        for service in data['services']:
            tlds = service[0]
            rdap_urls = service[1]
            for tld in tlds:
                writer.writerow([tld, rdap_urls[0]])

def main():
    data = fetch_iana_rdap()
    parse_and_save_rdap(data, 'tld_rdap_mapping.csv')
    print("Saved TLD RDAP mappings to tld_rdap_mapping.csv")

if __name__ == "__main__":
    main()
