#!/usr/bin/env node
const { chromium } = require('playwright');
const fs = require('fs').promises;
const path = require('path');

// Environment-tunable timeouts with sensible defaults
const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '2000');
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || '30000');
const DOWNLOAD_TIMEOUT_MS = parseInt(process.env.DOWNLOAD_TIMEOUT_MS || '300000'); // 5 minutes
const { getRunContext } = require('../lib/run-context');
const { finalizeRun } = require('../lib/manifest-utils');
const { captureDiagnostics, setupLogging, parseError } = require('../lib/diagnostics');
let RUNTIME = null;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Vendors don't have date filtering
function parseArgs(args) {
  if (args.length === 0) {
    // No arguments = export ALL vendors
    return {
      label: 'all'
    };
  }
  
  throw new Error(`Invalid number of arguments: ${args.length}. Vendor scraper only supports no arguments (exports all vendors).`);
}

async function scrapeVendors(label) {
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
    console.log('\nNevada ePro Vendor Scraper');
    console.log('===========================');
    console.log('Exporting: ALL VENDORS');
    
    stage = 'navigation';
    console.log('\nNavigating to Nevada ePro...');
    await page.goto('https://nevadaepro.com/bso/view/search/external/advancedSearchVendor.xhtml', {
      waitUntil: 'networkidle',
      timeout: NAV_TIMEOUT_MS
    });
    
    stage = 'search';
    console.log('Searching...');
    lastSelector = 'button:has-text("Search")';
    await page.locator(lastSelector).first().click();
    
    stage = 'wait_results';
    lastSelector = '.ui-datatable-tablewrapper, .ui-datatable-empty-message';
    // Wait for results to load (check for either results table or "No records found")
    await page.waitForSelector(lastSelector, { timeout: NAV_TIMEOUT_MS });
    
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
    lastSelector = 'img[src*="csv"]';
    await page.waitForSelector(lastSelector, { timeout: NAV_TIMEOUT_MS });
    
    stage = 'csv_export';
    console.log('Clicking CSV export...');
    const csvImage = await page.$(lastSelector);

    if (!csvImage) {
      console.log('⚠️  No CSV export icon found — likely no results to export.');
      return;
    }

    // Set up download promise BEFORE clicking (5 minute timeout for large exports)
    const downloadPromise = page.waitForEvent('download', { timeout: DOWNLOAD_TIMEOUT_MS });
    
    // Click the CSV image
    await csvImage.click();
    
    console.log('Waiting for download...');
    const download = await downloadPromise;
    
    stage = 'save_file';
    const outputPath = path.join(RUNTIME.OUTPUT_DIR, `vendor_${label}.csv`);
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
Nevada ePro Vendor Scraper CLI

Usage:
  pnpm run vendor              # Export ALL VENDORS (~19,500+ records)
  
Notes:
  - Vendor data has no date filtering
  - Exports all vendors in the system
  - Export includes: Vendor ID, Name, Address, City, State, Zip, Contact, Phone
`);
    return;
  }
  
  const { label } = parseArgs(args);
  
  // Initialize run context
  RUNTIME = getRunContext('vendors', label);
  
  console.log(`\n📁 Run ID: ${RUNTIME.runId}`);
  console.log(`📂 Output: ${RUNTIME.OUTPUT_DIR}`);
  
  const startTime = new Date();
  let status = 'success';
  let errorInfo = null;
  let result = null;
  
  try {
    result = await scrapeVendors(label);
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
    dataset: 'vendors',
    label,
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