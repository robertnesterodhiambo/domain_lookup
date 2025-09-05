#!/usr/bin/env python3
# pip install selenium webdriver-manager pandas

import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.domreg.lt/en/services/whois/?search=00000x0.lt"

def make_driver(headless=False, proxy=None):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--start-maximized")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    if proxy:
        opts.add_argument(f"--proxy-server=http://{proxy}")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def wait_for_load(driver, timeout=15):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def try_accept_cookies(driver):
    candidates = [
        (By.XPATH, "//button[normalize-space()='Accept']"),
        (By.XPATH, "//button[normalize-space()='I agree']"),
        (By.XPATH, "//button[contains(., 'Accept all')]"),
        (By.XPATH, "//button[contains(., 'I agree')]"),
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
        (By.XPATH, "//button[contains(., 'Sutinku')]"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, sel)))
            btn.click()
            return
        except Exception:
            continue

def parse_results(driver):
    # The result rows are in a table-like structure inside div#response
    rows = driver.find_elements(By.CSS_SELECTOR, "#response table tr")

    data = {}
    dns_list = []

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 2:
            key = cols[0].text.strip()
            value = cols[1].text.strip()
            if key.lower().startswith("ns"):  # DNS rows
                dns_list.append(value)
            else:
                data[key] = value

    # Assign DNS into dns1, dns2, ...
    for i, dns in enumerate(dns_list, start=1):
        data[f"dns{i}"] = dns

    return data

def main():
    driver = make_driver(headless=False)
    try:
        driver.get(URL)
        wait_for_load(driver)
        try_accept_cookies(driver)

        # Immediately click checkbox
        checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "isChecked"))
        )
        checkbox.click()

        # Click Show button
        show_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "show"))
        )
        show_button.click()

        # Wait for results container
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#response table"))
        )

        # Parse WHOIS data
        result = parse_results(driver)

        # Save to CSV
        df = pd.DataFrame([result])
        df.to_csv("data_rdap_parsed.csv", index=False)
        print("✔ Data saved to data_rdap_parsed.csv")

    finally:
        driver.quit()

if __name__ == "__main__":
    sys.exit(main())
