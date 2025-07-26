const { chromium } = require('playwright');
const fs = require('fs');
const csv = require('csv-parser');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

const INPUT_FILE = 'nslookup.csv';
const OUTPUT_FILE = 'page_count.csv';
const CONCURRENCY = 5;
const CHUNK_SIZE = 100;
const MAX_PAGES = 300;

// === MAIN THREAD ===
if (isMainThread) {
    const processed = new Set();
    let headerWritten = false;
    let headersFromInput = [];

    // Load already processed domains from OUTPUT_FILE
    if (fs.existsSync(OUTPUT_FILE)) {
        fs.readFileSync(OUTPUT_FILE, 'utf-8')
            .split('\n')
            .slice(1)
            .forEach(line => {
                const domain = line.split(',')[0]?.trim();
                if (domain) processed.add(domain);
            });
    }

    const outputStream = fs.createWriteStream(OUTPUT_FILE, { flags: 'a' });

    let rows = [];

    fs.createReadStream(INPUT_FILE)
        .pipe(csv())
        .on('headers', headers => {
            headersFromInput = headers;

            // Check if file is missing or empty, then write header
            if (!fs.existsSync(OUTPUT_FILE) || fs.readFileSync(OUTPUT_FILE, 'utf-8').trim().length === 0) {
                outputStream.write([...headers, 'pages_count'].join(',') + os.EOL);
                headerWritten = true;
            }
        })
        .on('data', (row) => {
            const domain = row.domain?.trim();
            if (domain && !processed.has(domain)) {
                rows.push(row);
            }
        })
        .on('end', async () => {
            console.log(`📥 ${rows.length} domains to process...`);

            for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
                const chunk = rows.slice(i, i + CHUNK_SIZE);
                await processChunk(chunk, outputStream, headersFromInput);
            }

            outputStream.close();
            console.log('✅ DONE');
        });

    async function processChunk(chunk, outputStream, headers) {
        return new Promise((resolve) => {
            let index = 0;
            let running = 0;

            function next() {
                if (index >= chunk.length && running === 0) return resolve();

                while (running < CONCURRENCY && index < chunk.length) {
                    const row = chunk[index++];
                    const domain = row.domain?.trim();
                    if (!domain) continue;

                    running++;

                    const worker = new Worker(__filename, {
                        workerData: { domain }
                    });

                    worker.on('message', (result) => {
                        if (result && result.domain) {
                            row.pages_count = result.count;
                            const values = headers.map(h => `"${String(row[h] || '').replace(/"/g, '""')}"`);
                            values.push(result.count);
                            outputStream.write(values.join(',') + os.EOL);
                            console.log(`✅ ${result.domain}: ${result.count} pages`);
                        }
                    });

                    worker.on('error', err => {
                        console.error(`❌ Error on ${domain}:`, err.message);
                    });

                    worker.on('exit', () => {
                        running--;
                        next();
                    });
                }
            }

            next();
        });
    }
}

// === WORKER THREAD ===
else {
    (async () => {
        const { domain } = workerData;
        const visited = new Set();
        const baseURL = `https://${domain}`;
        const browser = await chromium.launch({ headless: true });

        async function crawl(url) {
            if (visited.has(url) || visited.size >= MAX_PAGES || !url.startsWith(baseURL)) return;
            visited.add(url);

            try {
                const page = await browser.newPage();
                await page.goto(url, { waitUntil: 'networkidle' });
                await page.waitForTimeout(500);

                const links = await page.$$eval('a', anchors => anchors.map(a => a.href));
                const internalLinks = links.filter(href => href.startsWith(baseURL));
                await page.close();

                for (const link of internalLinks) {
                    await crawl(link);
                }
            } catch (err) {
                // ignore errors silently
            }
        }

        try {
            await crawl(baseURL);
        } catch (err) {
            // ignore errors silently
        }

        await browser.close();
        parentPort.postMessage({ domain, count: visited.size });
    })();
}
