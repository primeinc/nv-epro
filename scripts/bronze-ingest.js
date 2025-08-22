#!/usr/bin/env node

/**
 * Ingest all Nevada ePro datasets to Bronze layer
 * 
 * Processes:
 * - bids/bid_all.csv
 * - purchase_orders/po_all.csv
 * - contracts/contract_all.csv
 * - vendors/vendor_all.csv
 */

const { ingestToBronze } = require('../lib/bronze-ingestion');
const path = require('path');
const fs = require('fs').promises;

async function ingestAllDatasets() {
  const datasets = [
    {
      name: 'bids',
      file: 'bid_all.csv'
    },
    {
      name: 'purchase_orders',
      pattern: 'po_*.csv'  // Purchase orders use dated filenames
    },
    {
      name: 'contracts',
      file: 'contract_all.csv'
    },
    {
      name: 'vendors',
      file: 'vendor_all.csv'
    }
  ];
  
  // Find the latest run directory
  const rawBasePath = 'data/nevada-epro';
  const runId = `bronze_batch_${Date.now()}`;
  const results = [];
  
  console.log('🚀 Starting Bronze ingestion for all datasets\n');
  console.log('=' .repeat(60));
  
  for (const dataset of datasets) {
    console.log(`\n📊 Processing ${dataset.name}...`);
    console.log('-' .repeat(40));
    
    // Find the CSV file in the latest run
    const datasetPath = path.join(rawBasePath, dataset.name, 'raw/2025/08/21');
    
    try {
      // Get the latest run directory
      const runs = await fs.readdir(datasetPath);
      const latestRun = runs.sort().pop();
      
      if (!latestRun) {
        console.log(`⚠️  No data found for ${dataset.name}`);
        continue;
      }
      
      let csvPath;
      
      // Handle pattern-based files (like purchase orders)
      if (dataset.pattern) {
        const filesDir = path.join(datasetPath, latestRun, 'files');
        const files = await fs.readdir(filesDir);
        const matchingFiles = files.filter(f => 
          f.match(new RegExp(dataset.pattern.replace('*', '.*')))
        );
        
        if (matchingFiles.length === 0) {
          console.log(`⚠️  No files matching pattern: ${dataset.pattern}`);
          continue;
        }
        
        // Process each matching file
        for (const file of matchingFiles) {
          csvPath = path.join(filesDir, file);
          console.log(`   Processing: ${file}`);
          
          const result = await ingestToBronze(csvPath, dataset.name, {
            bronzeBasePath: 'data/bronze',
            runId,
            schemaVersion: 'v0.1.0'
          });
          
          results.push({
            dataset: dataset.name,
            file,
            ...result
          });
          
          if (result.skipped) {
            console.log(`   Status: Skipped (already ingested)`);
          } else {
            console.log(`   Status: Success`);
            console.log(`   Rows: ${result.row_count}`);
            console.log(`   Size: ${(result.source_file_bytes / 1024 / 1024).toFixed(2)} MB`);
          }
        }
        continue; // Skip to next dataset
      }
      
      // Handle single file datasets
      csvPath = path.join(datasetPath, latestRun, 'files', dataset.file);
      
      // Check if file exists
      try {
        await fs.access(csvPath);
      } catch {
        console.log(`⚠️  File not found: ${dataset.file}`);
        continue;
      }
      
      // Ingest to Bronze
      const result = await ingestToBronze(csvPath, dataset.name, {
        bronzeBasePath: 'data/bronze',
        runId,
        schemaVersion: 'v0.1.0'
      });
      
      results.push({
        dataset: dataset.name,
        ...result
      });
      
      if (result.skipped) {
        console.log(`   Status: Skipped (already ingested)`);
      } else {
        console.log(`   Status: Success`);
        console.log(`   Rows: ${result.row_count}`);
        console.log(`   Size: ${(result.source_file_bytes / 1024 / 1024).toFixed(2)} MB`);
      }
      
    } catch (error) {
      console.error(`❌ Failed to process ${dataset.name}: ${error.message}`);
      results.push({
        dataset: dataset.name,
        success: false,
        error: error.message
      });
    }
  }
  
  // Summary
  console.log('\n' + '=' .repeat(60));
  console.log('📈 Bronze Ingestion Summary\n');
  
  const successful = results.filter(r => r.success);
  const skipped = results.filter(r => r.skipped);
  const failed = results.filter(r => !r.success);
  
  console.log(`✅ Successful: ${successful.length - skipped.length} datasets`);
  console.log(`⏭️  Skipped: ${skipped.length} datasets (already ingested)`);
  if (failed.length > 0) {
    console.log(`❌ Failed: ${failed.length} datasets`);
  }
  
  console.log('\nDetails:');
  results.forEach(r => {
    if (r.success && !r.skipped) {
      console.log(`  ${r.dataset}: ${r.row_count} rows, ${r.source_file_hash?.substring(0, 8)}...`);
    } else if (r.skipped) {
      console.log(`  ${r.dataset}: Already ingested (${r.source_file_hash?.substring(0, 8)}...)`);
    } else {
      console.log(`  ${r.dataset}: Failed - ${r.error}`);
    }
  });
  
  // Write summary manifest
  const manifestPath = path.join('data/bronze', `manifest_${runId}.json`);
  await fs.writeFile(manifestPath, JSON.stringify({
    run_id: runId,
    timestamp: new Date().toISOString(),
    datasets_processed: results.length,
    successful: successful.length - skipped.length,
    skipped: skipped.length,
    failed: failed.length,
    results
  }, null, 2));
  
  console.log(`\n📝 Manifest written to: ${manifestPath}`);
}

// Run if called directly
if (require.main === module) {
  ingestAllDatasets().catch(console.error);
}

module.exports = { ingestAllDatasets };