#!/usr/bin/env node
const { chromium } = require('playwright');
const fs = require('fs').promises;
const path = require('path');

// Environment-tunable timeouts with sensible defaults
const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '2000');
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || '30000');

// Dynamic download timeout based on window size
// Can be overridden via environment variable
function getDownloadTimeout(startDate, endDate) {
  if (process.env.DOWNLOAD_TIMEOUT_MS) {
    return parseInt(process.env.DOWNLOAD_TIMEOUT_MS);
  }
  
  // Parse dates to determine window size
  const [startMonth, startDay, startYear] = startDate.split('/').map(Number);
  const [endMonth, endDay, endYear] = endDate.split('/').map(Number);
  
  // Single day: 30 seconds
  if (startYear === endYear && startMonth === endMonth && startDay === endDay) {
    return 30000; // 30 seconds
  }
  
  // Single month: 60 seconds (monthly windows are our standard)
  if (startYear === endYear && startMonth === endMonth) {
    return 60000; // 60 seconds
  }
  
  // Full year or multi-month: 2 minutes
  return 120000; // 2 minutes for larger windows
}
const { getRunContext } = require('./lib/run-context');
const { finalizeRun } = require('./lib/manifest-utils');
const { captureDiagnostics, setupLogging, parseError } = require('./lib/diagnostics');
let RUNTIME = null;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Month mapping
const MONTHS = {
  'jan': 1, 'january': 1, '1': 1,
  'feb': 2, 'february': 2, '2': 2,
  'mar': 3, 'march': 3, '3': 3,
  'apr': 4, 'april': 4, '4': 4,
  'may': 5, 'may': 5,
  'jun': 6, 'june': 6, '6': 6,
  'jul': 7, 'july': 7, '7': 7,
  'aug': 8, 'august': 8, '8': 8,
  'sep': 9, 'september': 9, '9': 9,
  'oct': 10, 'october': 10, '10': 10,
  'nov': 11, 'november': 11, '11': 11,
  'dec': 12, 'december': 12, '12': 12
};

const MONTH_NAMES = ['', 'january', 'february', 'march', 'april', 'may', 'june', 
                     'july', 'august', 'september', 'october', 'november', 'december'];

function getDaysInMonth(month, year) {
  return new Date(year, month, 0).getDate();
}

function validateDate(dateStr, label) {
  // Parse the date string (MM/DD/YYYY format)
  const [month, day, year] = dateStr.split('/').map(Number);
  const date = new Date(year, month - 1, day);
  
  // Earliest available date is January 31, 2018
  const earliestDate = new Date(2018, 0, 31); // January 31, 2018
  
  if (date < earliestDate) {
    throw new Error(`Invalid date: ${label}. Nevada ePro data only available from January 31, 2018 onwards.`);
  }
  
  // Don't allow future dates
  const today = new Date();
  today.setHours(23, 59, 59, 999);
  if (date > today) {
    throw new Error(`Invalid date: ${label}. Cannot retrieve data from the future.`);
  }
}

function parseArgs(args) {
  if (args.length === 0) {
    // Default to current month
    const now = new Date();
    return {
      startDate: `${String(now.getMonth() + 1).padStart(2, '0')}/01/${now.getFullYear()}`,
      endDate: `${String(now.getMonth() + 1).padStart(2, '0')}/${getDaysInMonth(now.getMonth() + 1, now.getFullYear())}/${now.getFullYear()}`,
      label: `${MONTH_NAMES[now.getMonth() + 1]}_${now.getFullYear()}`
    };
  }

  // American format parsing
  if (args.length === 3) {
    // Three args: month day year (e.g., aug 20 25)
    const monthArg = args[0].toLowerCase();
    const dayArg = parseInt(args[1]);
    const yearArg = parseInt(args[2]);
    
    if (!MONTHS[monthArg]) {
      throw new Error(`Invalid month: ${args[0]}`);
    }
    
    const month = MONTHS[monthArg];
    const year = yearArg < 100 ? 2000 + yearArg : yearArg;
    
    // Validate day
    const maxDays = getDaysInMonth(month, year);
    if (dayArg < 1 || dayArg > maxDays) {
      throw new Error(`Invalid day: ${dayArg}. ${MONTH_NAMES[month]} ${year} only has ${maxDays} days.`);
    }
    
    const monthStr = String(month).padStart(2, '0');
    const dayStr = String(dayArg).padStart(2, '0');
    const dateStr = `${monthStr}/${dayStr}/${year}`;
    
    // Validate date is not before Jan 31, 2018
    validateDate(dateStr, `${MONTH_NAMES[month]} ${dayArg}, ${year}`);
    
    return {
      startDate: dateStr,
      endDate: dateStr,
      label: `${year}-${monthStr}-${dayStr}`
    };
  }
  
  if (args.length === 2) {
    // Two args: month year (e.g., aug 25 or aug 2025)
    const monthArg = args[0].toLowerCase();
    const yearArg = parseInt(args[1]);
    
    // Check if first arg is a month
    if (MONTHS[monthArg]) {
      const month = MONTHS[monthArg];
      const year = yearArg < 100 ? 2000 + yearArg : yearArg;
      const lastDay = getDaysInMonth(month, year);
      const monthStr = String(month).padStart(2, '0');
      
      // Special case: Allow January 2018 even though data starts Jan 31
      // The website accepts jan 2018 and returns the one record from Jan 31
      if (!(month === 1 && year === 2018)) {
        // Validate start date for all other months
        validateDate(`${monthStr}/01/${year}`, `${MONTH_NAMES[month]} ${year}`);
      }
      
      return {
        startDate: `${monthStr}/01/${year}`,
        endDate: `${monthStr}/${lastDay}/${year}`,
        label: `${MONTH_NAMES[month]}_${year}`
      };
    }
    
    // Check if it's numeric month year (e.g., 8 2025)
    const monthNum = parseInt(args[0]);
    if (monthNum >= 1 && monthNum <= 12) {
      const year = yearArg < 100 ? 2000 + yearArg : yearArg;
      const lastDay = getDaysInMonth(monthNum, year);
      const monthStr = String(monthNum).padStart(2, '0');
      
      // Validate start date
      validateDate(`${monthStr}/01/${year}`, `${MONTH_NAMES[monthNum]} ${year}`);
      
      return {
        startDate: `${monthStr}/01/${year}`,
        endDate: `${monthStr}/${lastDay}/${year}`,
        label: `${MONTH_NAMES[monthNum]}_${year}`
      };
    }
    
    throw new Error(`Invalid arguments: ${args.join(' ')}`);
  }
  
  if (args.length === 1) {
    const arg = args[0].toLowerCase();
    const num = parseInt(args[0]);
    
    // Check if it's a year (2-digit or 4-digit)
    if (!isNaN(num)) {
      let year;
      if (num >= 18 && num <= 99) {
        year = 2000 + num;
      } else if (num >= 2018 && num <= 2099) {
        year = num;
      } else {
        throw new Error(`Invalid year: ${args[0]}. Data only available from 2018 onwards.`);
      }
      
      // Validate year is not before 2018
      if (year < 2018) {
        throw new Error(`Invalid year: ${year}. Nevada ePro data only available from January 31, 2018 onwards.`);
      }
      
      // For current year, end at today's date
      const today = new Date();
      let endDate;
      if (year === today.getFullYear()) {
        endDate = `${String(today.getMonth() + 1).padStart(2, '0')}/${String(today.getDate()).padStart(2, '0')}/${year}`;
      } else {
        endDate = `12/31/${year}`;
      }
      
      return {
        startDate: `01/01/${year}`,
        endDate: endDate,
        label: `year_${year}`
      };
    }
    
    // Check if it's a month name for current year
    if (MONTHS[arg]) {
      const month = MONTHS[arg];
      const year = new Date().getFullYear();
      const lastDay = getDaysInMonth(month, year);
      const monthStr = String(month).padStart(2, '0');
      
      return {
        startDate: `${monthStr}/01/${year}`,
        endDate: `${monthStr}/${lastDay}/${year}`,
        label: `${MONTH_NAMES[month]}_${year}`
      };
    }
    
    throw new Error(`Invalid argument: ${args[0]}. Expected month name (jan-dec) or year (18-99 or 2018+)`);
  }
  
  throw new Error(`Invalid number of arguments: ${args.length}`);
}

async function scrapePOs(startDate, endDate, label) {
  // Tracking variables for diagnostics
  let stage = 'initialization';
  let lastSelector = null;
  let browser = null;
  let context = null;
  let page = null;
  
  try {
    browser = await chromium.launch({ 
      headless: true,
      downloadsPath: RUNTIME.DOWNLOAD_DIR
    });
    
    const diagDir = path.join(RUNTIME.rawDir, 'diagnostics');
    await fs.mkdir(diagDir, { recursive: true }).catch(() => {});
    
    context = await browser.newContext({
      acceptDownloads: true,
      recordHar: { 
        path: path.join(diagDir, 'network.har'),
        content: 'embed'  // Embed response bodies
      }
    });
    
    // Start tracing for diagnostics
    await context.tracing.start({ 
      screenshots: true, 
      snapshots: true,
      sources: true
    });
    
    page = await context.newPage();
    
    // Set up logging
    setupLogging(page, RUNTIME);
    
    console.log('\nNevada ePro PO Scraper');
    console.log('======================');
    console.log(`Start Date: ${startDate}`);
    console.log(`End Date:   ${endDate}`);
    
    stage = 'navigation';
    console.log('\nNavigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchPurchaseOrder.xhtml', {
      waitUntil: 'networkidle',
      timeout: NAV_TIMEOUT_MS
    });
    
    stage = 'date_input';
    console.log('Setting date range...');
    lastSelector = 'input[id*="sentDateFrom_input"]';
    const fromDateInput = await page.locator(lastSelector);
    lastSelector = 'input[id*="sentDateTo_input"]';
    const toDateInput = await page.locator(lastSelector);
    
    await fromDateInput.clear();
    await fromDateInput.fill(startDate);
    
    await toDateInput.clear();
    await toDateInput.fill(endDate);
    
    await page.click('body');
    await sleep(500);
    
    stage = 'search';
    console.log('Searching...');
    lastSelector = 'button:has-text("Search")';
    await page.locator(lastSelector).first().click();
    
    stage = 'wait_results';
    lastSelector = '[id*="poResultId"]';
    await page.waitForSelector(lastSelector, { timeout: NAV_TIMEOUT_MS });
    await sleep(THROTTLE_MS);
    
    stage = 'csv_export';
    const downloadTimeout = getDownloadTimeout(startDate, endDate);
    console.log(`Clicking CSV export (timeout: ${downloadTimeout/1000}s)...`);
    const downloadPromise = page.waitForEvent('download', { timeout: downloadTimeout });
    await page.evaluate(() => {
      const images = Array.from(document.querySelectorAll('img'));
      const csvImage = images.find(img => img.src && img.src.includes('csv'));
      if (csvImage) {
        const link = csvImage.closest('a');
        if (link) link.click();
      }
    });
    
    const download = await downloadPromise;
    
    stage = 'save_file';
    const outputPath = path.join(RUNTIME.OUTPUT_DIR, `po_${label}.csv`);
    await download.saveAs(outputPath);
    
    let totalRecords = 0;
    try {
      await page.waitForSelector('.ui-paginator-current', { timeout: 5000 });
      const paginatorText = await page.textContent('.ui-paginator-current');
      const match = paginatorText?.match(/of\s+(\d+)/);
      totalRecords = match ? parseInt(match[1]) : 0;
    } catch (e) {
      console.log('Could not get record count from page');
    }
    
    console.log('\n✅ Success!');
    console.log(`Downloaded ${totalRecords} records to po_${label}.csv`);
    
    // Capture final URL for manifest
    const finalUrl = await page.url();
    
    // Save trace and HAR on success
    await context.tracing.stop({ 
      path: path.join(diagDir, 'trace.playwright.zip') 
    }).catch(() => {});
    
    return { success: true, totalRecords, finalUrl };
    
  } catch (error) {
    console.error('\n❌ Error during scraping:', error.message);
    
    // Capture all diagnostics on failure
    const errorInfo = parseError(error, stage, page, lastSelector);
    if (page) {
      try {
        errorInfo.url = await page.url();
      } catch {}
    }
    
    await captureDiagnostics(RUNTIME, context, page, error, stage);
    
    // Re-throw with enhanced error
    error.stage = stage;
    error.lastSelector = lastSelector;
    throw error;
    
  } finally {
    // Close context first to flush HAR
    if (context) {
      await context.close().catch(() => {});
    }
    if (browser) {
      await browser.close();
    }
  }
}

// Main
async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
Nevada ePro PO Scraper CLI

Usage:
  pnpm run po [month] [day] [year]
  
Examples:
  pnpm run po                  # Current month
  pnpm run po aug              # August of current year
  pnpm run po 2025             # All of 2025 (up to today if current year)
  pnpm run po 25               # All of 2025 (up to today if current year)
  pnpm run po 2017             # ERROR: No data before 2018
  pnpm run po aug 25           # All of August 2025
  pnpm run po aug 21           # All of August 2021
  pnpm run po 8 2025           # All of August 2025
  pnpm run po aug 20 25        # August 20, 2025 only (single day)
  pnpm run po aug 32 25        # ERROR: Invalid day
  
Format Rules:
  - 1 arg: year (18-99 or 2018+) OR month for current year
  - 2 args: month + year (whole month)
  - 3 args: month + day + year (single day)
  
Notes:
  - Month: jan-dec or 1-12
  - Day: 1-31 (validated for month)
  - Year: 18-99 (assumes 2000s) or full year (2018 minimum)
  - Data available from January 31, 2018 onwards
  - PO export REQUIRES date range (no "all records" support)
  - Use 'pnpm run bid' for bid data (supports no-date export)
`);
    return;
  }
  
  const { startDate, endDate, label } = parseArgs(args);
  
  // Initialize run context
  RUNTIME = getRunContext('purchase_orders', label);
  
  console.log(`\n📁 Run ID: ${RUNTIME.runId}`);
  console.log(`📂 Output: ${RUNTIME.OUTPUT_DIR}`);
  
  const startTime = new Date();
  let status = 'success';
  let errorInfo = null;
  let result = null;
  
  try {
    result = await scrapePOs(startDate, endDate, label);
  } catch (err) {
    status = 'error';
    errorInfo = {
      name: err.name || 'Error',
      message: String(err.message || err),
      stack: String(err.stack || ''),
      stage: err.stage || 'unknown',
      selector: err.lastSelector || null,
      timestamp: new Date().toISOString()
    };
    
    // Set exit code but don't throw - we want to finalize
    process.exitCode = 2;
  }
  
  const endTime = new Date();
  
  // Always finalize, even on error
  await finalizeRun(RUNTIME, {
    dataset: 'purchase_orders',
    label,
    startDate,
    endDate,
    startTime,
    endTime,
    status,
    error: errorInfo,
    finalUrl: result?.finalUrl,
    command: `node ${path.basename(__filename)} ${process.argv.slice(2).join(' ')}`
  });
  
  console.log('📋 Manifest: ' + path.relative(process.cwd(), RUNTIME.manifestPath));
  console.log('🔐 Checksums: ' + path.relative(process.cwd(), RUNTIME.checksumsPath));
  
  if (status === 'error') {
    console.log('⚠️  Diagnostics saved in: ' + path.relative(process.cwd(), path.join(RUNTIME.rawDir, 'diagnostics')));
    console.log('⚠️  Failure marker written: ' + path.relative(process.cwd(), path.join(RUNTIME.rawDir, '_FAILED')));
  }
}

if (require.main === module) {
  main().catch(console.error);
}