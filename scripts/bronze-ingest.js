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

async function getAllRuns(basePath) {
  const runs = [];
  async function* walkRuns(dir) {
    try {
      const entries = await fs.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          if (entry.name.startsWith('run_')) {
            runs.push(fullPath);
          } else {
            yield* walkRuns(fullPath);
          }
        }
      }
    } catch (e) {
      // Skip inaccessible directories
    }
  }
  
  for await (const _ of walkRuns(basePath)) {
    // Just collecting runs
  }
  return runs;
}

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
  
  const rawBasePath = 'data/nevada-epro';
  const bronzeBasePath = 'data/bronze';
  const runId = `bronze_batch_${Date.now()}`;
  const results = [];
  
  console.log('🚀 Starting Bronze ingestion for all datasets\n');
  console.log('=' .repeat(60));
  
  for (const dataset of datasets) {
    console.log(`\n📊 Processing ${dataset.name}...`);
    console.log('-' .repeat(40));
    
    // Find ALL run directories for this dataset
    const datasetRawPath = path.join(rawBasePath, dataset.name, 'raw');
    
    try {
      const allRuns = await getAllRuns(datasetRawPath);
      
      if (allRuns.length === 0) {
        console.log(`⚠️  No data found for ${dataset.name}`);
        continue;
      }
      
      console.log(`   Found ${allRuns.length} run(s) to process`);
      
      // Process each run
      for (const runPath of allRuns) {
        // Check if run was successful (no _FAILED marker)
        const failedMarker = path.join(runPath, '_FAILED');
        try {
          await fs.access(failedMarker);
          console.log(`   ⏭️  Skipping failed run: ${path.basename(runPath)}`);
          continue;
        } catch {
          // No _FAILED marker, proceed
        }
        
        const filesDir = path.join(runPath, 'files');
        
        // Check if files directory exists
        try {
          await fs.access(filesDir);
        } catch {
          console.log(`   ⚠️  No files directory in: ${path.basename(runPath)}`);
          continue;
        }
        
        // Handle pattern-based files (like purchase orders)
        if (dataset.pattern) {
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
          const csvPath = path.join(filesDir, file);
          console.log(`   Processing: ${file} from ${path.basename(runPath)}`);
          
          const result = await ingestToBronze(csvPath, dataset.name, {
            bronzeBasePath,
            runId: path.basename(runPath), // Use original run ID for traceability
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
      } else {
        // Handle single file datasets
        const csvPath = path.join(filesDir, dataset.file);
        
        // Check if file exists
        try {
          await fs.access(csvPath);
        } catch {
          console.log(`   ⚠️  File not found: ${dataset.file} in ${path.basename(runPath)}`);
          continue;
        }
        
        console.log(`   Processing: ${dataset.file} from ${path.basename(runPath)}`);
        
        // Ingest to Bronze
        const result = await ingestToBronze(csvPath, dataset.name, {
          bronzeBasePath,
          runId: path.basename(runPath), // Use original run ID for traceability
          schemaVersion: 'v0.1.0'
        });
      
        results.push({
          dataset: dataset.name,
          file: dataset.file,
          run: path.basename(runPath),
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
      } // End of run loop
      
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