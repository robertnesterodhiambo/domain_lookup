import pandas as pd
import requests
import time
import csv
import random

input_file = 'violations.csv'
output_file = 'complete.csv'

# Full list of your proxies (same repeated, can scale later)
proxy_credentials = [
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302",
    "proxy.geonode.io:9000:geonode_DrXb2XNsHm-type-residential:f232262f-0f34-400c-a7a6-84d1ce423302"
]

# Convert proxy credentials to requests-compatible format
proxy_list = []
for cred in proxy_credentials:
    host, port, username, password = cred.split(":")
    proxy_url = f"http://{username}:{password}@{host}:{port}"
    proxy_list.append({
        "http": proxy_url,
        "https": proxy_url
    })

geo_fields = ['city', 'region', 'country', 'loc', 'org', 'postal', 'timezone']
ip_cache = {}

def fetch_ipinfo(ip):
    try:
        return requests.get(f'https://ipinfo.io/{ip}/json', timeout=5)
    except:
        return None

def fetch_ipinfo_with_proxies(ip):
    for proxy in random.sample(proxy_list, len(proxy_list)):
        try:
            resp = requests.get(f'https://ipinfo.io/{ip}/json', timeout=10, proxies=proxy)
            if resp.status_code == 200:
                return resp
            else:
                print(f"Proxy failed for {ip} with status {resp.status_code}")
        except Exception as e:
            print(f"Proxy error for {ip}: {e}")
    return None

with open(output_file, mode='w', newline='', encoding='utf-8') as out_csv:
    writer = None

    for chunk in pd.read_csv(input_file, chunksize=100, quotechar='"', on_bad_lines='skip'):
        for _, row in chunk.iterrows():
            nslookupA = row.get('nslookupA', '')
            ip_list = [ip.strip() for ip in str(nslookupA).split('|') if ip.strip()] if pd.notna(nslookupA) else []
            
            geo_data = {field: '' for field in geo_fields}

            for ip in ip_list:
                if ip in ip_cache:
                    geo_data = ip_cache[ip]
                    break

                # First try direct request
                response = fetch_ipinfo(ip)
                if response and response.status_code == 200:
                    data = response.json()
                    geo_data = {
                        "city": data.get("city", ""),
                        "region": data.get("region", ""),
                        "country": data.get("country", ""),
                        "loc": data.get("loc", ""),
                        "org": data.get("org", ""),
                        "postal": data.get("postal", ""),
                        "timezone": data.get("timezone", "")
                    }
                    ip_cache[ip] = geo_data
                    break
                else:
                    print(f"Direct fetch failed for {ip}, trying proxies...")

                    response = fetch_ipinfo_with_proxies(ip)
                    if response and response.status_code == 200:
                        data = response.json()
                        geo_data = {
                            "city": data.get("city", ""),
                            "region": data.get("region", ""),
                            "country": data.get("country", ""),
                            "loc": data.get("loc", ""),
                            "org": data.get("org", ""),
                            "postal": data.get("postal", ""),
                            "timezone": data.get("timezone", "")
                        }
                        ip_cache[ip] = geo_data
                        break
                    else:
                        print(f"All proxy attempts failed for {ip}")
                        ip_cache[ip] = geo_data  # even if blank

                time.sleep(0.5)

            # Add geo columns to row
            for field in geo_fields:
                row[field] = geo_data[field]

            # Write header on first row
            if writer is None:
                writer = csv.DictWriter(out_csv, fieldnames=row.index)
                writer.writeheader()

            writer.writerow(row.to_dict())
