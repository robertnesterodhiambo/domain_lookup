import time
from urllib.parse import urlparse, urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

visited = set()
discovered = set()
max_depth = 5

def normalize_url(base_url, link):
    """Convert relative URL to absolute and strip trailing slash"""
    if not link or link.startswith("javascript") or link.startswith("#"):
        return None
    absolute = urljoin(base_url, link)
    parsed = urlparse(absolute)
    if parsed.scheme in ["http", "https"]:
        return parsed.scheme + "://" + parsed.netloc + parsed.path.rstrip("/")
    return None

def crawl_page(driver, url, depth):
    if depth > max_depth or url in visited:
        return

    visited.add(url)
    print(f"[{depth}] Crawling: {url}")

    try:
        driver.get(url)
        time.sleep(3)

        elements = driver.find_elements(By.TAG_NAME, "a")
        raw_links = [el.get_attribute("href") for el in elements]
        print(f"🔗 Found {len(raw_links)} links on {url}")

        base_domain = urlparse(url).netloc

        for raw_link in raw_links:
            normalized = normalize_url(url, raw_link)
            if not normalized:
                continue
            parsed = urlparse(normalized)
            if parsed.netloc == base_domain:
                if normalized not in discovered:
                    discovered.add(normalized)
                if normalized not in visited:
                    crawl_page(driver, normalized, depth + 1)

    except WebDriverException as e:
        print(f"❌ Error at {url}: {e}")

def main(start_url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        crawl_page(driver, start_url, 0)
    finally:
        driver.quit()
        print(f"\n✅ Total unique pages visited: {len(visited)}")
        print(f"📄 Total unique internal pages found: {len(discovered)}")

if __name__ == "__main__":
    main("https://jumia.com/")
