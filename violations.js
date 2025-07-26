const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');
const { chromium } = require('playwright');
const AxeBuilder = require('@axe-core/playwright').default;

const INPUT_CSV = 'page_count.csv';
const OUTPUT_CSV = 'violations.csv';
const CHUNK_SIZE = 100;

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

async function writeSingleResult(record) {
  const headers = Object.keys(record).map((key) => ({
    id: key,
    title: key
  }));

  const writer = createObjectCsvWriter({
    path: OUTPUT_CSV,
    header: headers,
    append: fileExists(OUTPUT_CSV)
  });

  await writer.writeRecords([record]); // Write one record immediately
}

async function processDomain(domain, originalRow) {
  const browser = await chromium.launch({ headless: true });
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

    await writeSingleResult(output); // ✅ Save immediately
  } catch (e) {
    console.log(`⚠️ Skipped ${url} due to error: ${e.message}`);

    const errorMessage = e.message.toLowerCase();
    let status = 'webpage not found'; // Default for any other error

    if (errorMessage.includes('err_address_unreachable')) {
      status = 'website unreachable';
    }

    const failedOutput = {
      ...originalRow,
      domain: domain,
      status: status
    };

    await writeSingleResult(failedOutput);
  } finally {
    await browser.close();
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

async function main() {
  const processed = await readProcessedDomains();

  readCSVInChunks(INPUT_CSV, CHUNK_SIZE, async (chunk) => {
    for (const row of chunk) {
      if (row.domain && !processed.has(row.domain.trim())) {
        await processDomain(row.domain.trim(), row);
      }
    }
  });
}

main();
