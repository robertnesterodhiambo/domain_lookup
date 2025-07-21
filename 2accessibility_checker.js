const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');

// Constants
const INPUT_FILE = 'db_excel_ns.csv';
const OUTPUT_FILE = 'accessibility.csv';

// Load axe-core source
const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

(async () => {
  // Launch browser
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();

  // Load already processed domains from output CSV (if exists)
  const processedDomains = new Set();
  const outputWorkbook = new ExcelJS.Workbook();
  let outputSheet;

  if (fs.existsSync(OUTPUT_FILE)) {
    await outputWorkbook.csv.readFile(OUTPUT_FILE);
    outputSheet = outputWorkbook.worksheets[0];
    for (let i = 2; i <= outputSheet.rowCount; i++) {
      const domain = outputSheet.getRow(i).getCell(1).text.trim();
      processedDomains.add(domain);
    }
  } else {
    outputSheet = outputWorkbook.addWorksheet('Results');
  }

  // Load input domains
  const inputWorkbook = new ExcelJS.Workbook();
  await inputWorkbook.csv.readFile(INPUT_FILE);
  const inputSheet = inputWorkbook.worksheets[0];

  // Add headers to output file if first time
  if (outputSheet.rowCount === 0) {
    const headers = inputSheet.getRow(1).values.slice(1); // Skip metadata
    headers.push('accessibility');
    outputSheet.addRow(headers);
    await outputWorkbook.csv.writeFile(OUTPUT_FILE);
  }

  for (let i = 2; i <= inputSheet.rowCount; i++) {
    const row = inputSheet.getRow(i);
    const rowData = row.values.slice(1); // Skip metadata
    const domain = row.getCell(1).text.trim();

    if (processedDomains.has(domain)) {
      console.log(`⏭️  Skipping already processed: ${domain}`);
      continue;
    }

    const url = `https://${domain}`;
    let accessibility = 'NO';

    try {
      console.log(`🔍 Checking ${url}`);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.evaluate(axeSource);
      const axeResults = await page.evaluate(async () => await axe.run());
      accessibility = axeResults.violations.length > 0 ? 'NO' : 'YES';
    } catch (err) {
      if (err.message.toLowerCase().includes('net::err_name_not_resolved')) {
        accessibility = 'UNREACHABLE';
      } else {
        console.error(`⚠️  Error on ${domain}: ${err.message}`);
        accessibility = 'NO';
      }
    }

    rowData.push(accessibility);
    outputSheet.addRow(rowData);
    processedDomains.add(domain);

    // Save immediately after each domain
    await outputWorkbook.csv.writeFile(OUTPUT_FILE);
    console.log(`✅ ${domain} => ${accessibility} (saved)`);
  }

  await browser.close();
  console.log(`🎉 Done. All results saved to ${OUTPUT_FILE}`);
})();
