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

  // Add new column headers if not present
  const firstRow = worksheet.getRow(1);
  const headers = firstRow.values.map(v => (typeof v === 'string' ? v.toLowerCase() : v));
  let accessColIndex = headers.indexOf('violations');

  if (accessColIndex === -1) {
    accessColIndex = firstRow.values.length + 1;
    firstRow.getCell(accessColIndex).value = 'Violations';
    firstRow.getCell(accessColIndex + 1).value = 'Passes';
    firstRow.getCell(accessColIndex + 2).value = 'Incomplete';
    firstRow.getCell(accessColIndex + 3).value = 'Inapplicable';
    firstRow.commit();
  }

  // Loop through rows
  for (let i = 2; i <= worksheet.rowCount; i++) {
    const row = worksheet.getRow(i);
    const domain = row.getCell(1).text.trim(); // assumes domain is in first column
    const url = `https://${domain}`;

    let violations = 'ERR';
    let passes = 'ERR';
    let incomplete = 'ERR';
    let inapplicable = 'ERR';

    try {
      console.log(`Checking ${url}`);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

      await page.evaluate(axeSource);
      const axeResults = await page.evaluate(async () => await axe.run());

      violations = axeResults.violations.length;
      passes = axeResults.passes.length;
      incomplete = axeResults.incomplete.length;
      inapplicable = axeResults.inapplicable.length;
    } catch (err) {
      const message = err.message.toLowerCase();
      console.error(`Error on ${domain}: ${err.message}`);
      if (message.includes('net::err_name_not_resolved')) {
        violations = passes = incomplete = inapplicable = 'UNREACHABLE';
      }
    }

    row.getCell(accessColIndex).value = violations;
    row.getCell(accessColIndex + 1).value = passes;
    row.getCell(accessColIndex + 2).value = incomplete;
    row.getCell(accessColIndex + 3).value = inapplicable;
    row.commit();

    console.log(`${domain} => Violations: ${violations}, Passes: ${passes}`);
  }

  await browser.close();
  await workbook.xlsx.writeFile(outputFile);
  console.log(`✅ Done. Results saved to ${outputFile}`);
})();
