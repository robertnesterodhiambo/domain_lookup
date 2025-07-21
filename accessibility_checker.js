const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');
const ExcelJS = require('exceljs');

// Load axe-core source
const axeSource = fs.readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');

const inputFile = 'db_excel_ns.csv';
const outputFile = 'accessibility.xlsx';

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();

  const workbook = new ExcelJS.Workbook();
  await workbook.csv.readFile(inputFile);
  const worksheet = workbook.worksheets[0];

  // Add new column header if not present
  const firstRow = worksheet.getRow(1);
  const headers = firstRow.values.map(v => (typeof v === 'string' ? v.toLowerCase() : v));
  let accessColIndex = headers.indexOf('accessibility');

  if (accessColIndex === -1) {
    accessColIndex = firstRow.values.length;
    firstRow.getCell(accessColIndex).value = 'accessibility';
    firstRow.commit();
  }

  // Loop through rows
  for (let i = 2; i <= worksheet.rowCount; i++) {
    const row = worksheet.getRow(i);
    const domain = row.getCell(1).text.trim(); // assumes domain is in first column
    const url = `https://${domain}`;
    let result = 'NO';

    try {
      console.log(`Checking ${url}`);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

      await page.evaluate(axeSource);
      const axeResults = await page.evaluate(async () => await axe.run());

      result = axeResults.violations.length > 0 ? 'NO' : 'YES';
    } catch (err) {
      const message = err.message.toLowerCase();
      if (message.includes('net::err_name_not_resolved')) {
        result = 'UNREACHABLE';
      } else {
        console.error(`Error on ${domain}: ${err.message}`);
        result = 'NO';
      }
    }

    row.getCell(accessColIndex).value = result;
    row.commit();
    console.log(`${domain} => ${result}`);
  }

  await browser.close();
  await workbook.xlsx.writeFile(outputFile);
  console.log(`✅ Done. Results saved to ${outputFile}`);
})();

