const fs = require('fs');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');

const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

const inputFile = 'db_excel_ns.csv';
const outputFile = 'accessibility.xlsx';
const completedFile = 'completed.txt';

const delay = ms => new Promise(res => setTimeout(res, ms));

async function tryLoadWithFallback(page, domain, maxRetries = 2) {
  const tryUrls = [`https://${domain}`, `http://${domain}`];

  for (let url of tryUrls) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
        return url;
      } catch (e) {
        if (attempt === maxRetries) {
          console.warn(`⚠️ Failed to load ${url} on attempt ${attempt}: ${e.message}`);
        } else {
          console.log(`🔁 Retry (${attempt}) after 5s...`);
          await delay(5000);
        }
      }
    }
  }

  throw new Error(`All attempts failed for ${domain}`);
}

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();

  // Load completed domains
  let completedDomains = new Set();
  if (fs.existsSync(completedFile)) {
    const content = fs.readFileSync(completedFile, 'utf8');
    completedDomains = new Set(content.split('\n').map(d => d.trim().toLowerCase()).filter(Boolean));
  }

  // Load domains from CSV
  const inputWorkbook = new ExcelJS.Workbook();
  await inputWorkbook.csv.readFile(inputFile);
  const inputSheet = inputWorkbook.worksheets[0];

  // Load or create output Excel
  const outputWorkbook = new ExcelJS.Workbook();
  let outputSheet;

  if (fs.existsSync(outputFile)) {
    await outputWorkbook.xlsx.readFile(outputFile);
    outputSheet = outputWorkbook.worksheets[0];
  } else {
    outputSheet = outputWorkbook.addWorksheet('Results');
    outputSheet.addRow([
      'Domain', 'Violations', 'Passes', 'Incomplete', 'Inapplicable'
    ]);
  }

  // Process each domain
  for (let i = 2; i <= inputSheet.rowCount; i++) {
    const row = inputSheet.getRow(i);
    const domain = row.getCell(1).text.trim().toLowerCase();

    if (!domain || completedDomains.has(domain)) {
      console.log(`⏭️ Skipping ${domain} (already in completed.txt)`);
      continue;
    }

    let violations = 'ERR';
    let passes = 'ERR';
    let incomplete = 'ERR';
    let inapplicable = 'ERR';

    try {
      console.log(`🌐 Checking ${domain}...`);
      const loadedUrl = await tryLoadWithFallback(page, domain);
      console.log(`✅ Loaded: ${loadedUrl}`);

      await page.evaluate(axeSource);
      const axeResults = await page.evaluate(async () => await axe.run());

      violations = axeResults.violations.length;
      passes = axeResults.passes.length;
      incomplete = axeResults.incomplete.length;
      inapplicable = axeResults.inapplicable.length;
    } catch (err) {
      console.error(`❌ Error for ${domain}: ${err.message}`);
      violations = passes = incomplete = inapplicable = 'UNREACHABLE';
    }

    // Only write row after analysis
    outputSheet.addRow([domain, violations, passes, incomplete, inapplicable]);

    // Append domain to completed.txt
    fs.appendFileSync(completedFile, domain + '\n');

    // Save after each domain
    await outputWorkbook.xlsx.writeFile(outputFile);
    console.log(`💾 Saved: ${domain} => V:${violations}, P:${passes}`);
  }

  await browser.close();
  console.log(`🎉 All done. Results written to ${outputFile}`);
})();
