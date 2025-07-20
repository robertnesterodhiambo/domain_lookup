from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time

# Set up Chrome options to use your local Chrome binary
chrome_options = Options()

# Specify your local Chrome installation path if needed:
# chrome_options.binary_location = "/usr/bin/google-chrome"  # default on many Linux systems
# For Windows: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"

# OPTIONAL: Remove automation flags to appear less bot-like
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Initialize driver using local Chrome with managed chromedriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Open the website
driver.get("https://domainmetadata.com/login")
time.sleep(5)  # Wait for the page to load fully

# Define email and password
email = "office@risikomonitor.com"
password = "sEAGgGqERE6z"

# Find the email input field and type letter by letter
email_input = driver.find_element(By.ID, "email")
for char in email:
    email_input.send_keys(char)
    time.sleep(0.1)  # Simulate typing speed

# Find the password input field and type letter by letter
password_input = driver.find_element(By.ID, "password")
for char in password:
    password_input.send_keys(char)
    time.sleep(0.1)  # Simulate typing speed

# Press Enter to submit
password_input.send_keys(Keys.ENTER)

# Wait for 6 seconds after login
time.sleep(60)

# Continue your automation or close driver
driver.quit()
