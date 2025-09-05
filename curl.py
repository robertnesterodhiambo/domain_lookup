import requests

url = "https://www.whois.com/whois/007.lt"

# Send a GET request like curl would
response = requests.get(url, headers={
    "User-Agent": "curl/7.68.0"  # mimic curl user-agent
})

# Print the response content
print(response.text)
