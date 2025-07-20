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
# chrome_options.binary_location = "/usr/bin/google-chrome"  # Uncomment if needed

# Optional: reduce automation detectability
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Initialize driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 10)

def login():
    driver.get("https://domainmetadata.com/login")
    time.sleep(5)  # Allow page to fully load

    attempt = 1
    max_attempts = 3

    while attempt <= max_attempts:
        print(f"Login attempt {attempt}...")

        email = "office@risikomonitor.com"
        password = "sEAGgGqERE6z"

        try:
            email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
            password_input = wait.until(EC.presence_of_element_located((By.ID, "password")))

            # Focus email input
            ActionChains(driver).move_to_element(email_input).click().perform()
            email_input.clear()
            for char in email:
                email_input.send_keys(char)
                time.sleep(0.1)
            email_input.send_keys(" ")  # trigger input event
            email_input.send_keys("\b")  # remove the extra space

            # Focus password input
            ActionChains(driver).move_to_element(password_input).click().perform()
            password_input.clear()
            for char in password:
                password_input.send_keys(char)
                time.sleep(0.1)
            password_input.send_keys(" ")  # trigger input event
            password_input.send_keys("\b")  # remove the extra space

            # Click Sign in button
            sign_in_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-lg.btn-primary")))
            sign_in_button.click()

            # Wait for either successful login or inputs still existing
            time.sleep(6)  # adjust based on network speed

            if not inputs_still_exist():
                print("Login successful.")
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

# Execute login
login()

# Continue your automation here

# Close driver at end
driver.quit()
