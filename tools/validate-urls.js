#!/usr/bin/env node

/**
 * URL Validation Tool for Nevada ePro Data
 * 
 * Validates that reconstructed URLs are working by testing samples from recent runs
 * Integrated into orchestration pipeline to catch URL pattern changes early
 */

const { chromium } = require('playwright');
const fs = require('fs').promises;
const path = require('path');
const { parse } = require('csv-parse/sync');
const { buildUrl, isValidIdFormat } = require('../lib/nevada-epro-url-builder');

// Helper to build bid holder list URLs
function buildBidHolderUrl(bidId) {
  return buildUrl(bidId, 'bid_holder_list');
}

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
const SAMPLE_SIZE = parseInt(process.env.URL_VALIDATION_SAMPLES || '5', 10);
const HEADLESS = process.env.URL_VALIDATION_HEADLESS !== 'false';

/**
 * Find the most recent successful run for a dataset
 */
async function findRecentRun(dataset) {
  const basePath = path.join(DATA_ROOT, 'nevada-epro', dataset, 'raw');
  
  try {
    // Get all year directories
    const years = (await fs.readdir(basePath))
      .filter(y => /^\d{4}$/.test(y))
      .sort()
      .reverse();
    
    for (const year of years) {
      const yearPath = path.join(basePath, year);
      const months = (await fs.readdir(yearPath))
        .filter(m => /^\d{2}$/.test(m))
        .sort()
        .reverse();
      
      for (const month of months) {
        const monthPath = path.join(yearPath, month);
        const days = (await fs.readdir(monthPath))
          .filter(d => /^\d{2}$/.test(d))
          .sort()
          .reverse();
        
        for (const day of days) {
          const dayPath = path.join(monthPath, day);
          const runs = (await fs.readdir(dayPath))
            .filter(r => r.startsWith('run_'))
            .sort()
            .reverse();
          
          for (const run of runs) {
            const manifestPath = path.join(dayPath, run, 'manifest.json');
            try {
              const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
              if (manifest.run?.status === 'success') {
                // Found a successful run - look for CSV files
                const filesDir = path.join(dayPath, run, 'files');
                const files = await fs.readdir(filesDir);
                const csvFile = files.find(f => f.endsWith('.csv') && !f.includes('_with_urls'));
                
                if (csvFile) {
                  return {
                    dataset,
                    runId: run,
                    date: `${year}-${month}-${day}`,
                    csvPath: path.join(filesDir, csvFile),
                    manifest
                  };
                }
              }
            } catch (e) {
              // Skip invalid manifests
            }
          }
        }
      }
    }
  } catch (e) {
    // Dataset directory doesn't exist
  }
  
  return null;
}

/**
 * Extract sample IDs from a CSV file
 */
async function extractSampleIds(csvPath, dataset, sampleSize, onlyWithHolderList = false) {
  const content = await fs.readFile(csvPath, 'utf8');
  
  // Parse CSV using csv-parse library (handles BOM, quotes, etc.)
  const records = parse(content, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true
  });
  
  if (records.length === 0) return [];
  
  const ids = [];
  const idColumn = getIdColumn(dataset);
  
  // Extract IDs from records
  for (let i = 0; i < records.length && ids.length < sampleSize; i++) {
    const record = records[i];
    const id = record[idColumn];
    
    if (id) {
      let cleanId = id;
      
      // For purchase orders, handle line item IDs like "99SWC-NV25-25281:2079"
      if (dataset === 'purchase_orders' && cleanId.includes(':')) {
        cleanId = cleanId.split(':')[0]; // Use base PO ID
      }
      
      // Filter for bids with holder lists if requested
      if (onlyWithHolderList && dataset === 'bids') {
        const holderListValue = record['Bid Holder List'];
        // Nevada ePro puts "/bso/external/bidAckList.sdo" when there's a public list
        if (holderListValue && holderListValue.includes('/bso/external/bidAckList.sdo')) {
          ids.push(cleanId);
        }
      } else {
        ids.push(cleanId);
      }
    }
  }
  
  return ids;
}

/**
 * Get the ID column name for a dataset
 */
function getIdColumn(dataset) {
  const columns = {
    'purchase_orders': 'PO #',
    'contracts': 'Contract #',
    'bids': 'Bid Solicitation #',
    'vendors': 'Vendor ID'
  };
  return columns[dataset] || 'ID';
}


/**
 * Test a URL to see if it loads successfully
 */
async function testUrl(page, url, recordType) {
  try {
    const response = await page.goto(url, { 
      waitUntil: 'domcontentloaded',
      timeout: 15000 
    });
    
    const status = response?.status();
    
    // Check for success indicators
    if (status === 200) {
      // Additional validation: check we're not on an error page
      const pageContent = await page.content();
      const hasError = pageContent.includes('Error Page') || 
                       pageContent.includes('not found');
      
      // For bid holder lists, check if it's a real list or "not authorized"
      if (recordType === 'bid_holder_list') {
        const notAuthorized = pageContent.includes('not authorized');
        const hasBidHolderList = pageContent.includes('Bid Holder List') && 
                                  pageContent.includes('Date Acknowledged');
        
        // Success if we get either a real list OR a not authorized message (both are valid)
        return { 
          success: true, 
          status,
          hasPublicList: hasBidHolderList,
          notAuthorized: notAuthorized
        };
      }
      
      return { success: !hasError, status };
    }
    
    return { success: false, status };
    
  } catch (error) {
    return { success: false, error: error.message };
  }
}

/**
 * Validate URLs for a dataset
 */
async function validateDataset(dataset, page) {
  const results = {
    dataset,
    tested: 0,
    passed: 0,
    failed: 0,
    errors: [],
    samples: []
  };
  
  // Find recent run
  const recentRun = await findRecentRun(dataset);
  if (!recentRun) {
    results.errors.push(`No recent successful runs found for ${dataset}`);
    return results;
  }
  
  // For bids, we need to read the full CSV to check holder list status
  let bidsWithHolderLists = new Set();
  if (dataset === 'bids') {
    const content = await fs.readFile(recentRun.csvPath, 'utf8');
    const records = parse(content, {
      columns: true,
      bom: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      trim: true
    });
    
    // Find bids that have holder lists
    for (const record of records) {
      if (record['Bid Holder List'] === '/bso/external/bidAckList.sdo') {
        bidsWithHolderLists.add(record['Bid Solicitation #']);
      }
    }
    console.log(`  Found ${bidsWithHolderLists.size} bids with public holder lists out of ${records.length} total`);
  }
  
  // Extract sample IDs for basic testing
  let ids = await extractSampleIds(recentRun.csvPath, dataset, SAMPLE_SIZE);
  if (ids.length === 0) {
    results.errors.push(`No valid IDs found in ${recentRun.csvPath}`);
    return results;
  }
  
  // For bids, ensure we include at least 2 bids with holder lists in our sample
  if (dataset === 'bids' && bidsWithHolderLists.size > 0) {
    const idsSet = new Set(ids);
    let holderListSamples = 0;
    
    // Count how many of our samples have holder lists
    for (const id of ids) {
      if (bidsWithHolderLists.has(id)) {
        holderListSamples++;
      }
    }
    
    // If we don't have at least 2, add some
    if (holderListSamples < 2) {
      const needed = 2 - holderListSamples;
      let added = 0;
      
      for (const bidWithList of bidsWithHolderLists) {
        if (!idsSet.has(bidWithList)) {
          ids.push(bidWithList);
          added++;
          if (added >= needed) break;
        }
      }
      
      console.log(`  Added ${added} bids with holder lists to ensure coverage`);
    }
  }
  
  console.log(`  Testing ${ids.length} ${dataset} URLs from ${recentRun.date}...`);
  
  // Test each ID
  for (const id of ids) {
    // Validate ID format
    const recordType = getRecordType(dataset);
    if (!isValidIdFormat(id, recordType)) {
      results.errors.push(`Invalid ID format: ${id}`);
      results.failed++;
      results.tested++;
      continue;
    }
    
    // Build and test primary URL
    const url = buildUrl(id, recordType);
    const result = await testUrl(page, url, recordType);
    
    results.tested++;
    if (result.success) {
      results.passed++;
      process.stdout.write('.');
    } else {
      results.failed++;
      results.errors.push(`${id}: ${result.error || `HTTP ${result.status}`}`);
      process.stdout.write('x');
    }
    
    results.samples.push({
      id,
      url,
      success: result.success,
      status: result.status,
      error: result.error
    });
    
    // For bids, test holder list URL ONLY if this bid has one
    if (dataset === 'bids' && bidsWithHolderLists.has(id)) {
      const holderUrl = buildUrl(id, 'bid_holder_list');
      const holderResult = await testUrl(page, holderUrl, 'bid_holder_list');
      
      // Count this as an additional test
      results.tested++;
      if (holderResult.success) {
        results.passed++;
        if (holderResult.hasPublicList) {
          process.stdout.write('H'); // Capital H for actual list
        } else {
          process.stdout.write('!'); // Should have had a list but didn't
        }
      } else {
        results.failed++;
        process.stdout.write('X'); // Failed when it should have worked
        results.errors.push(`${id} holder list: Should have worked but failed`);
      }
      
      results.samples.push({
        id: `${id} (holder list)`,
        url: holderUrl,
        success: holderResult.success,
        status: holderResult.status,
        note: 'Should have public holder list',
        error: holderResult.error
      });
      
      await page.waitForTimeout(500);
    }
    
    // Small delay to be nice to the server
    await page.waitForTimeout(500);
  }
  
  console.log('');
  return results;
}

/**
 * Get record type for URL building
 */
function getRecordType(dataset) {
  const mapping = {
    'purchase_orders': 'purchase_order',
    'contracts': 'contract',
    'bids': 'bid_detail',
    'vendors': 'vendor'
  };
  return mapping[dataset] || dataset;
}

/**
 * Main validation function
 */
async function validateUrls() {
  const startTime = Date.now();
  console.log('🔍 Nevada ePro URL Validation');
  console.log('==============================\n');
  
  const browser = await chromium.launch({ 
    headless: HEADLESS,
    timeout: 60000
  });
  
  const page = await browser.newPage();
  
  const datasets = ['purchase_orders', 'contracts', 'bids', 'vendors'];
  const results = {};
  let totalTested = 0;
  let totalPassed = 0;
  let totalFailed = 0;
  
  try {
    for (const dataset of datasets) {
      console.log(`\nValidating ${dataset}:`);
      const result = await validateDataset(dataset, page);
      results[dataset] = result;
      
      totalTested += result.tested;
      totalPassed += result.passed;
      totalFailed += result.failed;
      
      if (result.tested > 0) {
        const successRate = Math.round(result.passed / result.tested * 100);
        console.log(`  ✓ ${result.passed}/${result.tested} passed (${successRate}%)`);
        
        if (result.errors.length > 0 && result.errors.length <= 3) {
          result.errors.forEach(err => console.log(`    ⚠️  ${err}`));
        }
      } else {
        console.log(`  ⚠️  No samples tested: ${result.errors[0]}`);
      }
    }
    
    // Summary
    console.log('\n' + '='.repeat(40));
    console.log('VALIDATION SUMMARY');
    console.log('='.repeat(40));
    console.log(`Total URLs tested: ${totalTested}`);
    console.log(`Passed: ${totalPassed} (${totalTested > 0 ? Math.round(totalPassed/totalTested*100) : 0}%)`);
    console.log(`Failed: ${totalFailed}`);
    console.log(`Time: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
    
    // Write detailed report
    const reportPath = path.join(DATA_ROOT, 'url-validation-report.json');
    await fs.writeFile(reportPath, JSON.stringify({
      timestamp: new Date().toISOString(),
      summary: {
        totalTested,
        totalPassed,
        totalFailed,
        successRate: totalTested > 0 ? (totalPassed/totalTested*100).toFixed(2) + '%' : '0%',
        duration: `${((Date.now() - startTime) / 1000).toFixed(1)}s`
      },
      datasets: results,
      config: {
        sampleSize: SAMPLE_SIZE,
        dataRoot: DATA_ROOT,
        headless: HEADLESS
      }
    }, null, 2));
    
    console.log(`\n📊 Detailed report: ${path.relative(process.cwd(), reportPath)}`);
    
    // Exit with error if validation failed
    if (totalFailed > 0) {
      const failureRate = (totalFailed / totalTested * 100).toFixed(1);
      
      console.error(`\n❌ URL validation failed: ${failureRate}% failure rate`);
      console.error('The URL reconstruction patterns may have changed!');
      process.exitCode = 1;
    } else {
      console.log('\n✅ All URL patterns validated successfully!');
      
      // Show bid holder list statistics if available
      const bidResults = results.bids;
      if (bidResults && bidResults.samples) {
        const holderLists = bidResults.samples.filter(s => s.id.includes('holder list'));
        
        if (holderLists.length > 0) {
          const passed = holderLists.filter(s => s.success).length;
          const failed = holderLists.filter(s => !s.success).length;
          
          console.log(`\n📊 Bid Holder Lists validated: ${holderLists.length}`);
          console.log(`   ✅ ${passed} passed (URLs worked as expected)`);
          if (failed > 0) {
            console.log(`   ❌ ${failed} failed (should have worked)`);
          }
        }
      }
    }
    
  } catch (error) {
    console.error('\n❌ Validation error:', error.message);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

// Run if called directly
if (require.main === module) {
  validateUrls().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = { validateUrls };