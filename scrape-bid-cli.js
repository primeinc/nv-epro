#!/usr/bin/env node
const { chromium } = require('playwright');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const CONFIG = {
  OUTPUT_DIR: path.join(__dirname, 'output'),
  DOWNLOAD_DIR: path.join(__dirname, 'downloads'),
  THROTTLE_MS: 2000
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Month mapping
const MONTHS = {
  'jan': 1, 'january': 1, '1': 1,
  'feb': 2, 'february': 2, '2': 2,
  'mar': 3, 'march': 3, '3': 3,
  'apr': 4, 'april': 4, '4': 4,
  'may': 5, '5': 5,
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
  
  // Don't allow future dates
  const today = new Date();
  today.setHours(23, 59, 59, 999);
  if (date > today) {
    throw new Error(`Invalid date: ${label}. Cannot retrieve data from the future.`);
  }
}

function parseArgs(args) {
  if (args.length === 0) {
    // No arguments = export ALL bids (no date filtering)
    return {
      startDate: null,
      endDate: null,
      label: 'all'
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
      
      // Validate start date
      validateDate(`${monthStr}/01/${year}`, `${MONTH_NAMES[month]} ${year}`);
      
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

async function scrapeBids(startDate, endDate, label) {
  await fs.mkdir(CONFIG.DOWNLOAD_DIR, { recursive: true });
  await fs.mkdir(CONFIG.OUTPUT_DIR, { recursive: true });
  
  const browser = await chromium.launch({ 
    headless: true,
    downloadsPath: CONFIG.DOWNLOAD_DIR
  });
  
  const context = await browser.newContext({
    acceptDownloads: true
  });
  
  const page = await context.newPage();
  
  try {
    console.log('\nNevada ePro Bid Scraper');
    console.log('========================');
    if (startDate && endDate) {
      console.log(`Start Date: ${startDate}`);
      console.log(`End Date:   ${endDate}`);
    } else {
      console.log('Date Range: ALL BIDS (no date filter)');
    }
    
    console.log('\nNavigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchBid.xhtml', {
      waitUntil: 'networkidle',
      timeout: 30000
    });
    
    // Only fill dates if provided
    if (startDate && endDate) {
      console.log('Setting date range...');
      const fromDateInput = await page.locator('input[id="bidSearchForm:openingDateFrom_input"]');
      const toDateInput = await page.locator('input[id="bidSearchForm:openingDateTo_input"]');
      
      await fromDateInput.clear();
      await fromDateInput.fill(startDate);
      
      await toDateInput.clear();
      await toDateInput.fill(endDate);
      
      await page.click('body');
      await sleep(500);
    }
    
    console.log('Searching...');
    await page.locator('button:has-text("Search")').first().click();
    
    // Wait for the CSV export image to appear - that's all we need
    await page.waitForSelector('img[src*="csv"]', { timeout: 30000 });
    
    console.log('Clicking CSV export...');
    const csvImage = await page.$('img[src*="csv"]');

    if (!csvImage) {
      console.log('⚠️  No CSV export icon found — likely no results to export.');
      return;
    }

    // Set up download promise BEFORE clicking (5 minute timeout for large exports)
    const downloadPromise = page.waitForEvent('download', { timeout: 300000 });
    
    // Click the CSV image
    await csvImage.click();
    
    console.log('Waiting for download...');
    const download = await downloadPromise;
    
    const outputPath = path.join(CONFIG.OUTPUT_DIR, `bid_${label}.csv`);
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
    console.log(`Downloaded ${totalRecords} records to bid_${label}.csv`);
    
  } catch (error) {
    console.error('Error during scraping:', error);
    throw error;
  } finally {
    await browser.close();
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
  await scrapeBids(startDate, endDate, label);
}

if (require.main === module) {
  main().catch(console.error);
}