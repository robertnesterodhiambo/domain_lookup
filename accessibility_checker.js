
const fs = require('fs');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');
const os = require('os');

const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

const inputFile = 'page_count.csv';
const outputFile = 'complete.xlsx';
const completedFile = 'completed.txt';

const CONCURRENCY = 5;
const BATCH_SAVE_INTERVAL = 50;
const GOTO_TIMEOUT = 10000;

const delay = ms => new Promise(res => setTimeout(res, ms));

async function tryLoadWithFallback(page, domain, maxRetries = 2) {
  const tryUrls = [`https://${domain}`, `http://${domain}`];

  for (let url of tryUrls) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: GOTO_TIMEOUT });
        return url;
      } catch (e) {
        if (attempt === maxRetries) {
          return null;
        }
        await delay(2000);
      }
    }
  }

  return null;
}

async function analyzeDomain(browser, row, completedDomains) {
  const domain = row.getCell(1).text.trim().toLowerCase();

  if (!domain || completedDomains.has(domain)) {
    console.log(`⏭️ Skipping ${domain}`);
    return null;
  }

  const page = await browser.newPage();
  let result = {
    domain,
    violations: 'UNREACHABLE',
    passes: 'UNREACHABLE',
    incomplete: 'UNREACHABLE',
    inapplicable: 'UNREACHABLE',
    rowData: row.values.slice(1)
  };

  try {
    const url = await tryLoadWithFallback(page, domain);
    if (url) {
      console.log(`✅ Loaded: ${url}`);
      await page.evaluate(axeSource);
      const axeResults = await page.evaluate(async () => await axe.run());

      result.violations = axeResults.violations.length;
      result.passes = axeResults.passes.length;
      result.incomplete = axeResults.incomplete.length;
      result.inapplicable = axeResults.inapplicable.length;
    } else {
      console.log(`❌ Failed to load ${domain}`);
    }
  } catch (err) {
    console.log(`❌ Error with ${domain}: ${err.message}`);
  }

  await page.close();
  return result;
}

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  // Load completed
  let completedDomains = new Set();
  if (fs.existsSync(completedFile)) {
    const content = fs.readFileSync(completedFile, 'utf8');
    completedDomains = new Set(content.split('\n').map(d => d.trim().toLowerCase()).filter(Boolean));
  }

  // Load input
  const inputWorkbook = new ExcelJS.Workbook();
  await inputWorkbook.csv.readFile(inputFile);
  const inputSheet = inputWorkbook.worksheets[0];

  // Load/create output
  const outputWorkbook = new ExcelJS.Workbook();
  let outputSheet;
  if (fs.existsSync(outputFile)) {
    await outputWorkbook.xlsx.readFile(outputFile);
    outputSheet = outputWorkbook.worksheets[0];
  } else {
    outputSheet = outputWorkbook.addWorksheet('Results');
    const header = inputSheet.getRow(1).values.slice(1);
    outputSheet.addRow([...header, 'Violations', 'Passes', 'Incomplete', 'Inapplicable']);
  }

  // Collect rows to process
  const jobs = [];
  for (let i = 2; i <= inputSheet.rowCount; i++) {
    const row = inputSheet.getRow(i);
    const domain = row.getCell(1).text.trim().toLowerCase();
    if (!domain || completedDomains.has(domain)) continue;
    jobs.push(row);
  }

  console.log(`🚀 Starting scan for ${jobs.length} domains`);

  let index = 0;
  let processed = 0;

  while (index < jobs.length) {
    const batch = jobs.slice(index, index + CONCURRENCY);
    const results = await Promise.all(batch.map(row => analyzeDomain(browser, row, completedDomains)));

    for (const res of results) {
      if (res === null) continue;
      outputSheet.addRow([
        ...res.rowData,
        res.violations,
        res.passes,
        res.incomplete,
        res.inapplicable
      ]);
      fs.appendFileSync(completedFile, res.domain + os.EOL);
      processed++;
    }

    index += CONCURRENCY;

    if (processed % BATCH_SAVE_INTERVAL === 0 || index >= jobs.length) {
      await outputWorkbook.xlsx.writeFile(outputFile);
      console.log(`💾 Batch saved after ${processed} domains`);
    }
  }

  await browser.close();
  await outputWorkbook.xlsx.writeFile(outputFile);
  console.log(`🎉 Completed ${processed} domains. Output written to ${outputFile}`);
})();
