#!/usr/bin/env node

/**
 * Get the actual PO count from Nevada ePro website
 */

const { chromium } = require('playwright');

async function getEProPOCount(startDate = null, endDate = null, verbose = false) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  try {
    if (verbose) console.error('Navigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchPurchaseOrder.xhtml', {
      waitUntil: 'networkidle',
      timeout: 30000
    });
    
    // If date range provided, fill in the date fields
    if (startDate && endDate) {
      if (verbose) console.error(`Setting date range: ${startDate} to ${endDate}`);
      
      // Fill in date range (same selectors as scrape-po-cli.js)
      const fromDateInput = await page.locator('input[id*="sentDateFrom_input"]');
      const toDateInput = await page.locator('input[id*="sentDateTo_input"]');
      
      await fromDateInput.clear();
      await fromDateInput.fill(startDate);
      
      await toDateInput.clear();
      await toDateInput.fill(endDate);
      
      // Click outside to close any date pickers
      await page.click('body');
      await page.waitForTimeout(500);
    }
    
    if (verbose) console.error('Searching...');
    // Click search button
    await page.click('button:has-text("Search")');
    
    // Wait for results to load
    await page.waitForSelector('span.ui-paginator-current', { timeout: 30000 });
    
    // Get the paginator text
    const paginatorText = await page.textContent('span.ui-paginator-current');
    if (verbose) console.error(`Paginator text: "${paginatorText}"`);
    
    // Extract the total count (e.g., "1-25 of 96985")
    const match = paginatorText.match(/of\s+([\d,]+)/);
    if (match) {
      const total = parseInt(match[1].replace(/,/g, ''));
      if (verbose) console.error(`Extracted total: ${total}`);
      await browser.close();
      return total;
    } else {
      throw new Error(`Could not parse total from: ${paginatorText}`);
    }
  } catch (error) {
    if (verbose) console.error('Error:', error.message);
    await browser.close();
    throw error;
  }
}

// Export for use in other scripts
module.exports = { getEProPOCount };

// Run if called directly
if (require.main === module) {
  const args = process.argv.slice(2);
  const verbose = args.includes('--verbose') || args.includes('-v');
  
  // Remove flags from args
  const dateArgs = args.filter(arg => !arg.startsWith('-'));
  
  // Parse date arguments if provided (expecting MM/DD/YYYY MM/DD/YYYY format)
  let startDate = null;
  let endDate = null;
  
  if (dateArgs.length === 2) {
    // Validate date format
    const datePattern = /^\d{1,2}\/\d{1,2}\/\d{4}$/;
    if (dateArgs[0].match(datePattern) && dateArgs[1].match(datePattern)) {
      startDate = dateArgs[0];
      endDate = dateArgs[1];
    } else {
      console.error(JSON.stringify({
        error: 'Invalid date format. Expected: MM/DD/YYYY MM/DD/YYYY'
      }));
      process.exit(1);
    }
  } else if (dateArgs.length === 1 || dateArgs.length > 2) {
    console.error(JSON.stringify({
      error: 'Invalid arguments. Use: fetch-epro-live-count [start_date end_date] [--verbose]'
    }));
    process.exit(1);
  }
  
  getEProPOCount(startDate, endDate, verbose)
    .then(total => {
      const result = {
        timestamp: new Date().toISOString(),
        source: 'nevadaepro.com',
        dataset: 'purchase_orders',
        total: total
      };
      
      if (startDate && endDate) {
        result.date_range = {
          start: startDate,
          end: endDate
        };
      }
      
      console.log(JSON.stringify(result));
      process.exit(0);
    })
    .catch(error => {
      const errorResult = {
        timestamp: new Date().toISOString(),
        source: 'nevadaepro.com',
        dataset: 'purchase_orders',
        error: error.message
      };
      console.error(JSON.stringify(errorResult));
      process.exit(1);
    });
}