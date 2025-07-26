const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');
const { chromium } = require('playwright');
const AxeBuilder = require('@axe-core/playwright').default;

const INPUT_CSV = 'page_count.csv';
const OUTPUT_CSV = 'violations.csv';
const CHUNK_SIZE = 100;
const CONCURRENCY = 5;

function fileExists(file) {
  return fs.existsSync(file);
}

function readProcessedDomains() {
  return new Promise((resolve) => {
    const domains = new Set();
    if (!fileExists(OUTPUT_CSV)) return resolve(domains);

    fs.createReadStream(OUTPUT_CSV)
      .pipe(csv())
      .on('data', (row) => {
        if (row.domain) domains.add(row.domain.trim());
      })
      .on('end', () => resolve(domains));
  });
}

let csvHeadersInitialized = false;
let writer;

async function initCsvWriterIfNeeded(record) {
  if (csvHeadersInitialized) return;

  const headers = Object.keys(record).map((key) => ({
    id: key,
    title: key
  }));

  writer = createObjectCsvWriter({
    path: OUTPUT_CSV,
    header: headers,
    append: fileExists(OUTPUT_CSV)
  });

  csvHeadersInitialized = true;
}

async function writeSingleResult(record) {
  await initCsvWriterIfNeeded(record);
  await writer.writeRecords([record]);
}

async function processDomain(browser, domain, originalRow) {
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await context.newPage();
  const url = `https://${domain}`;

  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

    const results = await new AxeBuilder({ page }).analyze();
    const { violations, passes, incomplete } = results;

    console.log('\n==============================');
    console.log('🔎 Axe-core Accessibility Report');
    console.log(`🕸️ URL: ${url}`);
    console.log('==============================');
    console.log(`🚫 Violations:  ${violations.length}`);
    console.log(`✅ Passes:      ${passes.length}`);
    console.log(`⚠️ Incomplete:   ${incomplete.length}`);
    console.log('==============================\n');

    const output = {
      ...originalRow,
      violations: violations.length,
      passes: passes.length,
      incomplete: incomplete.length
    };

    await writeSingleResult(output);
  } catch (e) {
    console.log(`⚠️ Skipped ${url} due to error: ${e.message}`);

    const failedOutput = {
      ...originalRow,
      violations: 'site unreachable',
      passes: 'site unreachable',
      incomplete: 'site unreachable'
    };

    await writeSingleResult(failedOutput);
  } finally {
    await page.close();
    await context.close();
  }
}

function readCSVInChunks(filePath, chunkSize, callback) {
  const stream = fs.createReadStream(filePath).pipe(csv());
  let chunk = [];

  stream.on('data', (row) => {
    chunk.push(row);
    if (chunk.length === chunkSize) {
      stream.pause();
      callback(chunk).then(() => {
        chunk = [];
        stream.resume();
      });
    }
  });

  stream.on('end', () => {
    if (chunk.length > 0) callback(chunk).then(() => {
      console.log('✅ All data processed.');
    });
  });
}

async function runWithConcurrencyLimit(tasks, limit) {
  const results = [];
  const executing = [];

  for (const task of tasks) {
    const p = task().then((res) => {
      executing.splice(executing.indexOf(p), 1);
      return res;
    });
    results.push(p);
    executing.push(p);
    if (executing.length >= limit) {
      await Promise.race(executing);
    }
  }

  return Promise.all(results);
}

async function main() {
  const processed = await readProcessedDomains();
  const browser = await chromium.launch({ headless: true });

  readCSVInChunks(INPUT_CSV, CHUNK_SIZE, async (chunk) => {
    const tasks = [];

    for (const row of chunk) {
      if (row.domain && !processed.has(row.domain.trim())) {
        const domain = row.domain.trim();
        tasks.push(() => processDomain(browser, domain, row));
      }
    }

    await runWithConcurrencyLimit(tasks, CONCURRENCY);
  });

  process.on('exit', async () => {
    await browser.close();
  });
}

main();
