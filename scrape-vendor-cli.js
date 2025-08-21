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

// Vendors don't have date filtering, so we'll handle args for potential filtering
function parseArgs(args) {
  if (args.length === 0) {
    // No arguments = export ALL vendors
    return {
      filter: null,
      label: 'all'
    };
  }
  
  // Support single argument for filtering by state or starting letter
  if (args.length === 1) {
    const arg = args[0].toUpperCase();
    
    // Check if it's a state abbreviation (2 letters)
    if (arg.length === 2) {
      return {
        filter: { type: 'state', value: arg },
        label: `state_${arg.toLowerCase()}`
      };
    }
    
    // Check if it's a single letter for browsing
    if (arg.length === 1 && /[A-Z]/.test(arg)) {
      return {
        filter: { type: 'letter', value: arg },
        label: `letter_${arg.toLowerCase()}`
      };
    }
    
    throw new Error(`Invalid argument: ${args[0]}. Expected state abbreviation (e.g., NV) or letter (A-Z)`);
  }
  
  throw new Error(`Invalid number of arguments: ${args.length}. Use no args for all vendors, or provide a state/letter filter.`);
}

async function scrapeVendors(filter, label) {
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
    console.log('\nNevada ePro Vendor Scraper');
    console.log('===========================');
    if (filter) {
      console.log(`Filter: ${filter.type} = ${filter.value}`);
    } else {
      console.log('Filter: ALL VENDORS (no filter)');
    }
    
    console.log('\nNavigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchVendor.xhtml', {
      waitUntil: 'networkidle',
      timeout: 30000
    });
    
    // Apply filter if provided
    if (filter) {
      if (filter.type === 'state') {
        console.log('Setting state filter...');
        // Find and select the state dropdown
        const stateDropdown = await page.locator('select[id*="State"]').first();
        await stateDropdown.selectOption({ label: filter.value });
      } else if (filter.type === 'letter') {
        console.log('Clicking letter filter...');
        // Click the letter link for browsing
        await page.locator(`a:has-text("${filter.value}")`).first().click();
        await sleep(1000);
        // No need to click search after letter browse
        console.log('Letter browse applied, results loaded');
      }
    }
    
    // Only click search if not using letter browse
    if (!filter || filter.type !== 'letter') {
      console.log('Searching...');
      await page.locator('button:has-text("Search")').first().click();
    }
    
    // Wait for results to load (check for either results table or "No records found")
    await page.waitForSelector('.ui-datatable-tablewrapper, .ui-datatable-empty-message', { timeout: 30000 });
    
    // Check if we have "No records found"
    const noRecords = await page.$('.ui-datatable-empty-message');
    if (noRecords) {
      const text = await noRecords.textContent();
      if (text && text.includes('No records found')) {
        console.log('\n⚠️  No vendors found for the specified filter.');
        return;
      }
    }
    
    // Wait for the CSV export image to appear
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
    
    const outputPath = path.join(CONFIG.OUTPUT_DIR, `vendor_${label}.csv`);
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
    console.log(`Downloaded ${totalRecords} records to vendor_${label}.csv`);
    
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
Nevada ePro Vendor Scraper CLI

Usage:
  pnpm run vendor [filter]
  
Examples:
  pnpm run vendor              # ALL VENDORS (~19,500+ records)
  pnpm run vendor NV           # Vendors in Nevada
  pnpm run vendor CA           # Vendors in California  
  pnpm run vendor A            # Vendors starting with 'A'
  pnpm run vendor Z            # Vendors starting with 'Z'
  
Format Rules:
  - No args: ALL VENDORS (no filter)
  - 1 arg: State abbreviation (2 letters) or browse by letter (A-Z)
  
Notes:
  - Vendor data has no date filtering
  - State filter uses 2-letter state codes (NV, CA, TX, etc.)
  - Letter browse shows vendors starting with that letter
  - Export includes: Vendor ID, Name, Address, City, State, Zip, Contact, Phone
`);
    return;
  }
  
  const { filter, label } = parseArgs(args);
  await scrapeVendors(filter, label);
}

if (require.main === module) {
  main().catch(console.error);
}