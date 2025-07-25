const fs = require('fs');
const csv = require('csv-parser');
const { chromium } = require('playwright');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const CHUNK_SIZE = 5000;
const CONCURRENCY = 5;
const CSV_FILE = 'page_count.csv';
const OUTPUT_FILE = 'accessibility_results.csv';
const AXE_SCRIPT = fs.readFileSync('axe.min.js', 'utf-8');

const proxies = [
  { protocol: 'http', proxy: 'http://geonode_USER1:PASS1@proxy.geonode.io:9000' },
  { protocol: 'https', proxy: 'http://geonode_USER2:PASS2@proxy.geonode.io:9000' }
];

let headersWritten = false;
let csvWriter;

function initCsvWriter(headers) {
  csvWriter = createCsvWriter({
    path: OUTPUT_FILE,
    header: headers.map(h => ({ id: h, title: h })),
    append: fs.existsSync(OUTPUT_FILE),
    alwaysQuote: true
  });
}

function readDomainsInChunks(file, chunkSize) {
  return new Promise((resolve) => {
    const results = [];
    const chunks = [];
    fs.createReadStream(file)
      .pipe(csv())
      .on('headers', (headers) => {
        if (!headersWritten) {
          headersWritten = true;
          initCsvWriter([...headers, 'passCount', 'violationCount', 'incompleteCount', 'status', 'error']);
        }
      })
      .on('data', (data) => {
        results.push(data);
        if (results.length === chunkSize) {
          chunks.push(results.splice(0));
        }
      })
      .on('end', () => {
        if (results.length) chunks.push(results);
        resolve(chunks);
      });
  });
}

async function scanDomain(record, browser) {
  const url = record.domain.startsWith('http') ? record.domain : `http://${record.domain}`;

  for (let i = 0; i < proxies.length; i++) {
    const context = await browser.newContext({
      proxy: { server: proxies[i].proxy }
    });

    try {
      const page = await context.newPage();
      const response = await page.goto(url, { timeout: 15000 });
      const status = response?.status();
      const finalUrl = page.url();
      console.log(`🧭 Navigated to: ${url} | Status: ${status} | Final: ${finalUrl}`);

      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(2000); // Give extra time for dynamic pages

      const title = await page.title();
      console.log(`📄 Page Title: ${title}`);

      await page.addScriptTag({ content: AXE_SCRIPT });
      console.log('✅ Axe script injected');

      const result = await page.evaluate(async () => {
        return await axe.run(document, {
          runOnly: {
            type: 'tag',
            values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice']
          },
          resultTypes: ['violations', 'passes', 'incomplete']
        });
      });

      console.log(`🔍 Axe Result — Passes: ${result.passes.length}, Violations: ${result.violations.length}, Incomplete: ${result.incomplete.length}`);

      const output = {
        ...record,
        passCount: result.passes.length,
        violationCount: result.violations.length,
        incompleteCount: result.incomplete.length,
        status: '✅',
        error: ''
      };

      await csvWriter.writeRecords([output]);
      await context.close();
      return;
    } catch (err) {
      console.log(`❌ Failed: ${url} with proxy ${proxies[i].protocol}`);
      console.log(`   Error: ${err.message}`);
      await context.close();

      if (i === proxies.length - 1) {
        const output = {
          ...record,
          passCount: '',
          violationCount: '',
          incompleteCount: '',
          status: '❌',
          error: err.message
        };
        await csvWriter.writeRecords([output]);
      }
    }
  }
}

async function runConcurrentScans(records, browser, concurrency = CONCURRENCY) {
  let index = 0;

  async function worker() {
    while (index < records.length) {
      const i = index++;
      await scanDomain(records[i], browser);
    }
  }

  const workers = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
}

(async () => {
  const domainChunks = await readDomainsInChunks(CSV_FILE, CHUNK_SIZE);
  const browser = await chromium.launch({ headless: true });

  for (const chunk of domainChunks) {
    console.log(`🔄 Processing ${chunk.length} domains...`);
    await runConcurrentScans(chunk, browser);
  }

  await browser.close();
  console.log('✅ Finished all domain scans');
})();
