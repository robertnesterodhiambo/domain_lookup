import requests
import time
from itertools import cycle

PROXY_SCRAPE_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&timeout=5000&ssl=yes"
TEST_URL = "https://rdap.sidn.nl/domain/123-auto.nl"
ROTATION_INTERVAL = 15 * 60  # 15 minutes

def fetch_https_proxies():
    try:
        response = requests.get(PROXY_SCRAPE_URL, timeout=10)
        raw_proxies = response.text.strip().split('\n')
        clean_proxies = []
        for line in raw_proxies:
            line = line.strip()
            if not line:
                continue
            # ProxyScrape returns only IP:PORT, prepend with "socks4://" if needed
            clean_proxies.append(f"socks4://{line}")
        return clean_proxies
    except Exception as e:
        print(f"Error fetching proxy list: {e}")
        return []

def test_proxy(proxy):
    try:
        proxies = {
            "http": proxy,
            "https": proxy,
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; RDAPBot/1.0; +https://example.com/bot)'
        }
        response = requests.get(TEST_URL, proxies=proxies, headers=headers, timeout=10)
        if response.status_code == 200:
            print(f"[+] Successful connection with proxy: {proxy}")
            return True
        else:
            print(f"[-] Proxy failed (status {response.status_code}): {proxy}")
    except Exception as e:
        print(f"[-] Proxy failed with error: {e}")
    return False

def main():
    print("Fetching proxy list...")
    proxy_list = fetch_https_proxies()

    if not proxy_list:
        print("No proxies found. Exiting.")
        return

    proxy_pool = cycle(proxy_list)

    for proxy in proxy_pool:
        print(f"\n[*] Trying proxy: {proxy}")
        if test_proxy(proxy):
            print(f"[+] Waiting 15 minutes before switching proxy...")
            time.sleep(ROTATION_INTERVAL)
        else:
            print("[!] Trying next proxy...")

if __name__ == "__main__":
    main()
