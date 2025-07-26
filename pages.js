const { chromium } = require('playwright');

const visited = new Set();
const maxPages = 300; // Max number of pages to crawl
const baseDomain = 'https://0pstap.nl';

async function crawl(url, browser) {
    if (visited.has(url) || visited.size >= maxPages || !url.startsWith(baseDomain)) return;
    visited.add(url);

    try {
        const page = await browser.newPage();
        await page.goto(url, { waitUntil: 'networkidle' });
        console.log(`🔗 ${url}`);
        await page.waitForTimeout(500); // give JS time to load

        // Get all hrefs
        const links = await page.$$eval('a', anchors =>
            anchors.map(a => a.href)
        );

        // Filter internal links only (outside browser context)
        const internalLinks = links.filter(href =>
            href.startsWith('https://0pstap.nl')
        );

        await page.close();

        // Recursively crawl internal links
        for (const link of internalLinks) {
            await crawl(link, browser);
        }

    } catch (err) {
        console.error(`❌ Error on ${url}:`, err.message);
    }
}

(async () => {
    const browser = await chromium.launch({ headless: true });
    await crawl(baseDomain, browser);
    await browser.close();

    console.log(`\n✅ TOTAL UNIQUE PAGES FOUND: ${visited.size}`);
})();
