const fs = require('fs');
const csv = require('csv-parser');
const { chromium } = require('playwright');
const { HttpProxyAgent, HttpsProxyAgent } = require("hpagent");
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const CHUNK_SIZE = 500;
const CSV_FILE = 'page_count.csv';
const OUTPUT_FILE = 'accessibility_results.csv';
const AXE_SCRIPT = fs.readFileSync('axe.min.js', 'utf-8');

// Your proxy configurations
const proxies = [
  {
    protocol: 'http',
    proxy: 'http://geonode_USER1:PASS1@proxy.geonode.io:9000'
  },
  {
    protocol: 'https',
    proxy: 'http://geonode_USER2:PASS2@proxy.geonode.io:9000'
  }
];

// Setup CSV writer (headers inferred dynamically)
let headersWritten = false;
let csvWriter;

function initCsvWriter(headers) {
  csvWriter = createCsvWriter({
    path: OUTPUT_FILE,
    header: headers.map(h => ({ id: h, title: h })),
    append: fs.existsSync(OUTPUT_FILE),
    alwaysQuote: true  // ✅ Ensures commas and special chars in fields are preserved correctly
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
          initCsvWriter([...headers, 'passCount', 'violations', 'incomplete', 'status', 'error']);
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
  let url = record.domain.startsWith('http') ? record.domain : `http://${record.domain}`;
  for (let i = 0; i < proxies.length; i++) {
    const context = await browser.newContext({
      proxy: {
        server: proxies[i].proxy
      }
    });

    try {
      const page = await context.newPage();
      await page.goto(url, { timeout: 15000 });

      await page.addScriptTag({ content: AXE_SCRIPT });

      const result = await page.evaluate(async () => await axe.run());

      const output = {
        ...record,
        passCount: result.passes.length,
        violations: result.violations.length,
        incomplete: result.incomplete.length,
        status: '✅',
        error: ''
      };

      console.log(`✅ ${url}`);
      await csvWriter.writeRecords([output]);
      await context.close();
      return;
    } catch (err) {
      console.log(`❌ ${url} with proxy ${proxies[i].protocol}`);
      console.log(`   Error: ${err.message}`);
      await context.close();

      if (i === proxies.length - 1) {
        const output = {
          ...record,
          passCount: '',
          violations: '',
          incomplete: '',
          status: '❌',
          error: err.message
        };
        await csvWriter.writeRecords([output]);
      }
    }
  }
}

(async () => {
  const domainChunks = await readDomainsInChunks(CSV_FILE, CHUNK_SIZE);
  const browser = await chromium.launch({ headless: true });

  for (const chunk of domainChunks) {
    for (const record of chunk) {
      await scanDomain(record, browser);
    }
  }

  await browser.close();
})();
