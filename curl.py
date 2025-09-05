import requests
from bs4 import BeautifulSoup

url = "https://www.whois.com/whois/007.lt"

# Fetch the page
response = requests.get(url, headers={"User-Agent": "curl/7.68.0"})

# Parse the HTML
soup = BeautifulSoup(response.text, "html.parser")

# The WHOIS data is inside <pre id="registryData">
whois_block = soup.find("pre", id="registryData")

if whois_block:
    print(whois_block.get_text())
else:
    print("WHOIS data not found.")
