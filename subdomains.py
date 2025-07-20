import pandas as pd
import csv
import ast
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, urljoin
import time
from datetime import datetime

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_page_load_timeout(15)  # Timeout in 15 seconds
    return driver

# --- Clean date fields ---
def clean_date_cell(cell):
    try:
        # Only try parsing if it looks like a list or contains 'datetime.'
        if "[" in cell and "datetime." in cell:
            parsed = ast.literal_eval(cell)
            formatted = []
            for item in parsed:
                if isinstance(item, datetime):
                    formatted.append(str(item))
                elif isinstance(item, str) and item.startswith("datetime."):
                    try:
                        dt = eval(item)
                        formatted.append(str(dt))
                    except:
                        continue
            return "; ".join(formatted)
        else:
            return cell
    except Exception:
        return cell

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

# === Safe CSV Loader ===
rows = []
with open("whois_results.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    num_columns = len(header)

    for row in reader:
        if len(row) > num_columns:
            # Collapse extra data into the last column
            row = row[:num_columns-1] + [",".join(row[num_columns-1:])]
        elif len(row) < num_columns:
            # Pad with empty strings if too short
            row += [""] * (num_columns - len(row))
        rows.append(row)

df = pd.DataFrame(rows, columns=header)

# Drop rows with 'NO_DATA' in raw_text
df = df[df['raw_text'] != 'NO_DATA']

# Clean date columns
date_columns = ['Creation_date', 'Expiration_date', 'Updated_date']
for col in date_columns:
    if col in df.columns:
        df[col] = df[col].astype(str).apply(clean_date_cell)

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
