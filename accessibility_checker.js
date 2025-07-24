const fs = require('fs');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');
const os = require('os');
const csv = require('csv-parser');

const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

const inputFile = 'page_count.csv';
const outputFile = 'complete.xlsx';
const completedFile = 'completed.txt';

const CONCURRENCY = 5;
const BATCH_SAVE_INTERVAL = 50;
const GOTO_TIMEOUT = 10000;
const CHUNK_SIZE = 50000;

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

async function analyzeDomain(browser, rowData, completedDomains) {
  const domain = rowData[0].trim().toLowerCase();

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
    rowData
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

async function processChunk(browser, chunk, outputSheet, completedDomains, outputWorkbook) {
  let index = 0;
  let processed = 0;

  while (index < chunk.length) {
    const batch = chunk.slice(index, index + CONCURRENCY);
    const results = await Promise.all(batch.map(rowData => analyzeDomain(browser, rowData, completedDomains)));

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
      completedDomains.add(res.domain);
      processed++;
    }

    index += CONCURRENCY;

    if (processed % BATCH_SAVE_INTERVAL === 0 || index >= chunk.length) {
      await outputWorkbook.xlsx.writeFile(outputFile);
      console.log(`💾 Batch saved after ${processed} domains`);
    }
  }
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

  // Load/create output
  const outputWorkbook = new ExcelJS.Workbook();
  let outputSheet;
  if (fs.existsSync(outputFile)) {
    await outputWorkbook.xlsx.readFile(outputFile);
    outputSheet = outputWorkbook.worksheets[0];
  } else {
    outputSheet = outputWorkbook.addWorksheet('Results');
  }

  // Stream input in chunks
  let chunk = [];
  let headerAdded = false;

  const processStream = async () => {
    if (chunk.length === 0) return;

    // If header not yet added, do so
    if (!headerAdded) {
      outputSheet.addRow([...chunk[0], 'Violations', 'Passes', 'Incomplete', 'Inapplicable']);
      headerAdded = true;
      chunk.shift(); // remove header row from processing
    }

    console.log(`🚀 Processing chunk of ${chunk.length} domains`);
    await processChunk(browser, chunk, outputSheet, completedDomains, outputWorkbook);
    chunk = [];
  };

  await new Promise((resolve, reject) => {
    fs.createReadStream(inputFile)
      .pipe(csv())
      .on('data', (row) => {
        const rowData = Object.values(row);
        chunk.push(rowData);
        if (chunk.length >= CHUNK_SIZE) {
          processStream().catch(reject);
        }
      })
      .on('end', async () => {
        await processStream();
        resolve();
      })
      .on('error', reject);
  });

  await browser.close();
  await outputWorkbook.xlsx.writeFile(outputFile);
  console.log(`🎉 All chunks processed. Output written to ${outputFile}`);
})();
