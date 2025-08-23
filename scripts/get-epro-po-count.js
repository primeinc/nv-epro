#!/usr/bin/env node

/**
 * Get the actual PO count from Nevada ePro website
 */

const { chromium } = require('playwright');

async function getEProPOCount(verbose = false) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  try {
    if (verbose) console.error('Navigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchPurchaseOrder.xhtml', {
      waitUntil: 'networkidle',
      timeout: 30000
    });
    
    if (verbose) console.error('Searching for all POs...');
    // Click search without any filters to get all POs
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
  const verbose = process.argv.includes('--verbose') || process.argv.includes('-v');
  getEProPOCount(verbose)
    .then(total => {
      const result = {
        timestamp: new Date().toISOString(),
        source: 'nevadaepro.com',
        dataset: 'purchase_orders',
        total: total
      };
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