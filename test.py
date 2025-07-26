import asyncio
from urllib.parse import urlparse
from playwright.async_api import async_playwright

visited = set()
max_depth = 5  # You can increase this

async def crawl_page(context, url, depth):
    if depth > max_depth or url in visited:
        return

    visited.add(url)
    print(f"[{depth}] Crawling: {url}")

    try:
        page = await context.new_page()
        await page.goto(url, timeout=20000)
        await page.wait_for_timeout(3000)  # wait for JS to load

        hrefs = await page.eval_on_selector_all("a", "elements => elements.map(el => el.href)")
        print(f"🔗 Found {len(hrefs)} links on {url}")

        base_domain = urlparse(url).netloc
        for link in hrefs:
            parsed = urlparse(link)
            if parsed.scheme in ["http", "https"] and parsed.netloc == base_domain:
                await crawl_page(context, link, depth + 1)

        await page.close()

    except Exception as e:
        print(f"❌ Error at {url}: {e}")

async def main(start_url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await crawl_page(context, start_url, 0)
        await browser.close()
        print(f"\n✅ Total unique pages visited: {len(visited)}")

if __name__ == "__main__":
    asyncio.run(main("https://google.com/"))
