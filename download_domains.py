from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time

# Set up Chrome options if needed
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Uncomment for headless

# Initialize driver with webdriver_manager
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
    time.sleep(0.1)  # Adjust typing speed if needed

# Find the password input field and type letter by letter
password_input = driver.find_element(By.ID, "password")
for char in password:
    password_input.send_keys(char)
    time.sleep(0.1)  # Adjust typing speed if needed

# Press Enter to submit
password_input.send_keys(Keys.ENTER)

# Wait for 6 seconds after login
time.sleep(6)

# Continue your automation or close driver
driver.quit()
