import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Setup download directory
download_dir = os.path.join(os.getcwd(), "domain_zip")
os.makedirs(download_dir, exist_ok=True)

# Setup Chrome options
chrome_options = Options()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 10)

def login():
    driver.get("https://domainmetadata.com/login")
    time.sleep(5)

    email = "office@risikomonitor.com"
    password = "sEAGgGqERE6z"
    attempt = 1
    max_attempts = 3

    while attempt <= max_attempts:
        try:
            print(f"Login attempt {attempt}...")

            email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
            password_input = wait.until(EC.presence_of_element_located((By.ID, "password")))

            ActionChains(driver).move_to_element(email_input).click().perform()
            email_input.clear()
            for char in email:
                email_input.send_keys(char)
                time.sleep(0.05)

            ActionChains(driver).move_to_element(password_input).click().perform()
            password_input.clear()
            for char in password:
                password_input.send_keys(char)
                time.sleep(0.05)

            sign_in_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-lg.btn-primary")))
            sign_in_button.click()
            time.sleep(6)

            if not inputs_still_exist():
                print("Login successful.")
                driver.get("https://domainmetadata.com/list-of-all-domains")
                time.sleep(5)
                scroll_to_bottom()
                links = collect_links_via_keyboard()
                download_files(links)
                return
            else:
                print("Login failed, retrying...")
                attempt += 1

        except (NoSuchElementException, TimeoutException) as e:
            print("Error locating login elements:", e)
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

def collect_links_via_keyboard():
    collected_links = []

    try:
        driver.get("https://domainmetadata.com/list-of-all-domains")
        time.sleep(3)
        scroll_to_bottom()
        dropdown_buttons = driver.find_elements(By.CSS_SELECTOR, "div.dropdown > button")
        print(f"Found {len(dropdown_buttons)} dropdown buttons.")

        # We'll only store the positions (indices), not the elements themselves
        dropdown_indices = list(range(1, len(dropdown_buttons)))  # skip index 0

        for i in dropdown_indices:
            try:
                driver.get("https://domainmetadata.com/list-of-all-domains")
                time.sleep(2)
                scroll_to_bottom()

                dropdown_buttons = driver.find_elements(By.CSS_SELECTOR, "div.dropdown > button")

                if i >= len(dropdown_buttons):
                    print(f"Index {i} out of range after reload, skipping.")
                    continue

                btn = dropdown_buttons[i]
                btn_text = btn.text.strip().lower()
                if any(word in btn_text for word in ["logout", "account", "profile"]):
                    print(f"Skipping dropdown {i} with label: '{btn_text}'")
                    continue

                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(0.5)
                ActionChains(driver).move_to_element(btn).click().perform()
                time.sleep(1)

                ActionChains(driver)\
                    .send_keys(Keys.ARROW_DOWN)\
                    .pause(0.5)\
                    .send_keys(Keys.ARROW_DOWN)\
                    .pause(0.5)\
                    .send_keys(Keys.ENTER)\
                    .perform()

                time.sleep(4)
                current_url = driver.current_url
                if "domainmetadata.com" in current_url and current_url not in collected_links:
                    print("Collected link:", current_url)
                    collected_links.append(current_url)

            except Exception as e:
                print(f"Error processing dropdown {i}:", e)

        print(f"Total links collected: {len(collected_links)}")
        return collected_links

    except Exception as e:
        print("Error during link collection:", e)
        return []

def download_files(links):
    if not links:
        print("No links to download.")
        return

    for link in links:
        print(f"Visiting and downloading: {link}")
        driver.get(link)
        time.sleep(5)

# Run it
login()
driver.quit()
