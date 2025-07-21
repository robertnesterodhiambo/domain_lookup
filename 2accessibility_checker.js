const fs = require('fs');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');
const path = require('path');

const INPUT_FILE = 'db_excel_ns.csv';
const OUTPUT_FILE = 'accessibility.csv';
const MAX_CONCURRENT_PAGES = 20;

const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Mutex to prevent concurrent writes
let writing = false;
async function waitForWriteRelease() {
  while (writing) await delay(50);
}

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  // Load processed domains
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

  // Prepare header
  const headerRow = inputSheet.getRow(1).values.slice(1); // skip meta
  if (outputSheet.rowCount === 0) {
    headerRow.push('accessibility');
    outputSheet.addRow(headerRow);
    await outputWorkbook.csv.writeFile(OUTPUT_FILE);
  }

  // Domains to process
  const domainTasks = [];

  for (let i = 2; i <= inputSheet.rowCount; i++) {
    const row = inputSheet.getRow(i);
    const rowData = row.values.slice(1); // skip meta
    const domain = row.getCell(1).text.trim();

    if (!processedDomains.has(domain)) {
      domainTasks.push({ domain, rowData });
    }
  }

  console.log(`🔧 Total domains to process: ${domainTasks.length}`);

  // Worker function
  async function processDomain(task) {
    const { domain, rowData } = task;
    const page = await browser.newPage();
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
        console.error(`⚠️ Error on ${domain}: ${err.message}`);
        accessibility = 'NO';
      }
    }

    await page.close();

    // Write result immediately with lock
    await waitForWriteRelease();
    writing = true;
    rowData.push(accessibility);
    outputSheet.addRow(rowData);
    await outputWorkbook.csv.writeFile(OUTPUT_FILE);
    writing = false;

    console.log(`✅ ${domain} => ${accessibility} (saved)`);
  }

  // Run tasks in batches with limited concurrency
  let index = 0;
  async function runBatch() {
    while (index < domainTasks.length) {
      const batch = domainTasks.slice(index, index + MAX_CONCURRENT_PAGES);
      index += MAX_CONCURRENT_PAGES;
      await Promise.all(batch.map(task => processDomain(task)));
    }
  }

  await runBatch();
  await browser.close();
  console.log('🎉 Done. All results saved to accessibility.csv');
})();
