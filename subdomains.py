from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse

def extract_subdomains(url, root_domain):
    try:
        html = requests.get(url, timeout=5).text
        soup = BeautifulSoup(html, 'html.parser')
        
        subdomains = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http'):
                parsed = urlparse(href)
                domain = parsed.netloc
                if domain.endswith(root_domain) and domain != root_domain:
                    subdomains.add(domain)
        
        return subdomains
    except Exception as e:
        print(f"Error: {e}")
        return set()

# Example usage
url = "https://github.com"
root_domain = "github.com"
found_subdomains = extract_subdomains(url, root_domain)

print(f"Found {len(found_subdomains)} subdomain(s):")
for subdomain in found_subdomains:
    print(subdomain)
