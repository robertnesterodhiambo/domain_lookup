#!/usr/bin/env python3
# pip install selenium webdriver-manager

import sys
import time
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

def wait_for_load(driver, timeout=20):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def try_accept_cookies(driver):
    try:
        wait = WebDriverWait(driver, 5)
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
                btn = wait.until(EC.element_to_be_clickable((by, sel)))
                btn.click()
                time.sleep(0.5)
                return
            except Exception:
                pass
    except Exception:
        pass

def main():
    driver = make_driver(headless=False)
    try:
        driver.get(URL)
        wait_for_load(driver)
        try_accept_cookies(driver)

        # Click the checkbox
        checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "isChecked"))
        )
        checkbox.click()
        print("✔ Checkbox clicked")

        # Click the Show button
        show_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "show"))
        )
        show_button.click()
        print("✔ Show button clicked")

        # Wait a moment for results to load
        time.sleep(3)

        print("Title:", driver.title)
        print("Current URL:", driver.current_url)

    finally:
        driver.quit()

if __name__ == "__main__":
    sys.exit(main())
