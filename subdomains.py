import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, urljoin
import time

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_page_load_timeout(15)  # Timeout in 15 seconds
    return driver

def extract_subdomains_selenium(driver, root_domain):
    subdomains = set()
    base_url = driver.current_url

    try:
        # Scroll down and up to trigger all content
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)

        anchors = driver.find_elements("tag name", "a")
        for a in anchors:
            href = a.get_attribute("href")
            if href:
                full_url = urljoin(base_url, href)
                domain = urlparse(full_url).netloc
                if domain.endswith(root_domain) and domain != root_domain:
                    subdomains.add(domain)
    except Exception as e:
        print(f"Error during extraction on {root_domain}: {e}")
    return len(subdomains)

# === Load original CSV and filter ===
df = pd.read_csv("whois_results.csv")

# Drop rows with 'NO_DATA' in raw_text
df = df[df['raw_text'] != 'NO_DATA']

# Add subdomain count column if missing
if 'subdomain count' not in df.columns:
    df['subdomain count'] = None

# Work on a copy so original is untouched
output_csv = "subdomain.csv"
df_copy = df.copy()

# Start browser
driver = get_driver()

# Process each domain
for i, row in df_copy.iterrows():
    domain = row['domain']
    current_count = row['subdomain count']

    if pd.isna(current_count) or str(current_count).strip() == "":
        try:
            url = f"https://{domain}"
            print(f"Processing: {domain}")
            driver.get(url)
            count = extract_subdomains_selenium(driver, domain)
        except TimeoutException:
            print(f"{domain} - TIMED OUT after 15s")
            count = 0
        except Exception as e:
            print(f"{domain} - Error: {e}")
            count = 0

        # Update count and save immediately
        df_copy.at[i, 'subdomain count'] = count
        print(f"{domain}: {count} subdomains")
        df_copy.to_csv(output_csv, index=False)

# Clean up
driver.quit()
