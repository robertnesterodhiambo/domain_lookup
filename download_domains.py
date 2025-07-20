import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time

# Setup Chrome options
chrome_options = Options()
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Initialize driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 10)

def login():
    driver.get("https://domainmetadata.com/login")
    time.sleep(5)

    attempt = 1
    max_attempts = 3

    while attempt <= max_attempts:
        print(f"Login attempt {attempt}...")

        email = "office@risikomonitor.com"
        password = "sEAGgGqERE6z"

        try:
            email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
            password_input = wait.until(EC.presence_of_element_located((By.ID, "password")))

            # Type email
            ActionChains(driver).move_to_element(email_input).click().perform()
            email_input.clear()
            for char in email:
                email_input.send_keys(char)
                time.sleep(0.1)
            email_input.send_keys(" ")
            email_input.send_keys("\b")

            # Type password
            ActionChains(driver).move_to_element(password_input).click().perform()
            password_input.clear()
            for char in password:
                password_input.send_keys(char)
                time.sleep(0.1)
            password_input.send_keys(" ")
            password_input.send_keys("\b")

            # Click Sign in button
            sign_in_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-lg.btn-primary")))
            sign_in_button.click()

            time.sleep(6)

            if not inputs_still_exist():
                print("Login successful.")
                driver.get("https://domainmetadata.com/list-of-all-domains")
                time.sleep(5)
                scroll_to_bottom()
                download_links = collect_second_li_links()
                download_files(download_links)
                break
            else:
                print("Login failed, retrying...")
                attempt += 1

        except (NoSuchElementException, TimeoutException) as e:
            print("Error locating element:", e)
            break

def inputs_still_exist():
    try:
        driver.find_element(By.ID, "email")
        driver.find_element(By.ID, "password")
        return True
    except NoSuchElementException:
        return False

def scroll_to_bottom():
    print("Scrolling to the bottom of the page...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    print("Reached bottom of the page.")

def collect_second_li_links():
    collected_links = []

    try:
        dropdown_buttons = driver.find_elements(By.CSS_SELECTOR, "div.dropdown > button")
        buttons_count = len(dropdown_buttons)
        print(f"Found {buttons_count} dropdown buttons.")

        for i in range(buttons_count):
            # Refetch buttons every time to avoid stale element reference
            dropdown_buttons = driver.find_elements(By.CSS_SELECTOR, "div.dropdown > button")
            btn = dropdown_buttons[i]

            # Scroll into view and click to open dropdown menu
            driver.execute_script("arguments[0].scrollIntoView(true);", btn)
            time.sleep(0.5)
            ActionChains(driver).move_to_element(btn).click().perform()
            time.sleep(1)

        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "ul.dropdown-menu")))

        ul_menus = driver.find_elements(By.CSS_SELECTOR, "ul.dropdown-menu")

        for ul in ul_menus:
            li_items = ul.find_elements(By.TAG_NAME, "li")
            if len(li_items) >= 2:
                second_li = li_items[1]
                try:
                    a_tag = second_li.find_element(By.TAG_NAME, "a")
                    href = a_tag.get_attribute("href")
                    if href.startswith("/"):
                        href = "https://domainmetadata.com" + href
                    collected_links.append(href)
                except NoSuchElementException:
                    print("Second <li> has no <a> tag.")

        print(f"Collected {len(collected_links)} links from second <li> of each dropdown menu.")
        for link in collected_links:
            print("Link:", link)

        return collected_links

    except (NoSuchElementException, TimeoutException) as e:
        print("Error collecting second <li> links:", e)
        return []

def download_files(links):
    if not links:
        print("No links to download.")
        return

    os.makedirs("domain_zip", exist_ok=True)
    session = requests.Session()

    for link in links:
        filename = os.path.basename(link)
        filepath = os.path.join("domain_zip", filename)
        print(f"Downloading {link} to {filepath} ...")

        try:
            resp = session.get(link)
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(resp.content)
            print(f"Saved {filepath}")
        except requests.RequestException as e:
            print(f"Failed to download {link}: {e}")

# Run the script
login()
driver.quit()
