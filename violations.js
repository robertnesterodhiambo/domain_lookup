const { chromium } = require('playwright');
const AxeBuilder = require('@axe-core/playwright').default;

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  const url = 'https://0pstap.nl/';
  await page.goto(url, { waitUntil: 'domcontentloaded' });

  const results = await new AxeBuilder({ page }).analyze();
  const { violations, passes, incomplete } = results;

  // Output clean summary
  console.log('\n==============================');
  console.log('🔎 Axe-core Accessibility Report');
  console.log(`🕸️ URL: ${url}`);
  console.log('==============================');
  console.log(`🚫 Violations:  ${violations.length}`);
  console.log(`✅ Passes:      ${passes.length}`);
  console.log(`⚠️ Incomplete:   ${incomplete.length}`);
  console.log('==============================\n');

  await browser.close();
})();
