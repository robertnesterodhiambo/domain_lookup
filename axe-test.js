const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// Load axe-core source
const axeSource = fs.readFileSync(
  require.resolve('axe-core/axe.min.js'),
  'utf8'
);

// The URL you want to test
const TEST_URL = process.argv[2] || 'https://example.com';

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();

  try {
    await page.goto(TEST_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Inject and run axe
    await page.evaluate(axeSource);
    const results = await page.evaluate(async () => {
      return await axe.run();
    });

    const hasViolations = results.violations.length > 0;

    // Print YES or NO only
    console.log(hasViolations ? 'NO' : 'YES');

    // Optional: Save full report
    const reportPath = path.join(__dirname, 'axe-report.json');
    fs.writeFileSync(reportPath, JSON.stringify(results, null, 2));

  } catch (err) {
    console.error('ERROR:', err.message);
    console.log('NO');
  } finally {
    await browser.close();
  }
})();

