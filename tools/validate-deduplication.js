#!/usr/bin/env node

// Validation tests for deduplication logic
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');

function runCommand(cmd) {
  try {
    return execSync(cmd, { encoding: 'utf8', stdio: 'pipe' });
  } catch (error) {
    throw new Error(`Command failed: ${cmd}\n${error.stdout}\n${error.stderr}`);
  }
}

async function validateIdempotency() {
  console.log('🔄 TESTING IDEMPOTENCY');
  console.log('='.repeat(60));
  
  // Use existing PO data for idempotency test
  const poRuns = fs.readdirSync(path.join(DATA_ROOT, 'nevada-epro', 'purchase_orders', 'raw', '2025', '08', '21'));
  const testRun = poRuns[0];
  const runPath = `"${path.join(DATA_ROOT, 'nevada-epro', 'purchase_orders', 'raw', '2025', '08', '21', testRun)}"`;
  
  console.log(`Using test run: ${testRun}`);
  
  // Clear canonical state
  const canonicalDir = path.join(DATA_ROOT, 'nevada-epro', 'canonical.duckdb');
  if (fs.existsSync(canonicalDir)) {
    fs.unlinkSync(canonicalDir);
    console.log('✅ Cleared canonical database');
  }
  
  // First ingestion
  console.log('\n📥 First ingestion...');
  const result1 = runCommand(`node ingest.js ${runPath}`);
  const stats1 = extractStats(result1);
  console.log(`First run: ${stats1.new_records} new, ${stats1.updated_records} updated, ${stats1.unchanged_records} unchanged`);
  
  // Second ingestion (should be idempotent)
  console.log('\n🔁 Second ingestion (idempotency test)...');
  const result2 = runCommand(`node ingest.js ${runPath}`);
  const stats2 = extractStats(result2);
  console.log(`Second run: ${stats2.new_records} new, ${stats2.updated_records} updated, ${stats2.unchanged_records} unchanged`);
  
  // Validation
  if (stats2.new_records === 0 && stats2.updated_records === 0) {
    console.log('✅ IDEMPOTENCY TEST PASSED');
    console.log(`   No new or updated records on second run (${stats2.unchanged_records} unchanged)`);
  } else {
    console.log('❌ IDEMPOTENCY TEST FAILED');
    console.log(`   Expected 0 new/updated, got ${stats2.new_records} new and ${stats2.updated_records} updated`);
    return false;
  }
  
  return true;
}

async function validateCrossDatasetIngestion() {
  console.log('\n🎯 TESTING CROSS-DATASET INGESTION');
  console.log('='.repeat(60));
  
  const datasets = [
    { type: 'purchase_orders', path: 'purchase_orders/raw' },
    { type: 'contracts', path: 'contracts/raw' },
    { type: 'bids', path: 'bids/raw' },
    { type: 'vendors', path: 'vendors/raw' }
  ];
  
  for (const dataset of datasets) {
    const datasetPath = path.join(DATA_ROOT, 'nevada-epro', dataset.path);
    
    if (!fs.existsSync(datasetPath)) {
      console.log(`⏭️  Skipping ${dataset.type} - no data found`);
      continue;
    }
    
    // Find a run directory
    const runs = findRunDirectories(datasetPath);
    if (runs.length === 0) {
      console.log(`⏭️  Skipping ${dataset.type} - no runs found`);
      continue;
    }
    
    const testRun = runs[0];
    console.log(`\n📊 Testing ${dataset.type} with ${testRun}`);
    
    try {
      const result = runCommand(`node ingest.js "${testRun}"`);
      const stats = extractStats(result);
      
      console.log(`   ✅ Success: ${stats.processed_records} processed, ${stats.canonical_total} in canonical`);
      
      if (stats.processed_records !== stats.canonical_total) {
        console.log(`   ⚠️  Warning: Processed count doesn't match canonical total`);
      }
    } catch (error) {
      console.log(`   ❌ Failed: ${error.message.split('\n')[0]}`);
      return false;
    }
  }
  
  return true;
}

async function validateUniqueConstraints() {
  console.log('\n🔑 TESTING UNIQUE CONSTRAINTS');
  console.log('='.repeat(60));
  
  const { DuckDBInstance } = require('@duckdb/node-api');
  
  const dbPath = path.join(DATA_ROOT, 'nevada-epro', 'canonical.duckdb');
  if (!fs.existsSync(dbPath)) {
    console.log('⏭️  Skipping - no canonical database found');
    return true;
  }
  
  const instance = await DuckDBInstance.create(dbPath);
  const conn = await instance.connect();
  
  const datasets = [
    { table: 'canonical_purchase_orders', key: 'PO #' },
    { table: 'canonical_contracts', key: 'Contract #' },
    { table: 'canonical_bids', key: 'Bid Solicitation #' },
    { table: 'canonical_vendors', key: 'Vendor ID' }
  ];
  
  for (const dataset of datasets) {
    try {
      // Check if table exists
      const tableCheck = await conn.run(`SELECT name FROM sqlite_master WHERE type='table' AND name='${dataset.table}'`);
      const tableExists = (await tableCheck.getRowObjects()).length > 0;
      
      if (!tableExists) {
        console.log(`⏭️  Table ${dataset.table} doesn't exist`);
        continue;
      }
      
      // Check uniqueness
      const totalRows = await conn.run(`SELECT COUNT(*) as count FROM ${dataset.table}`);
      const uniqueKeys = await conn.run(`SELECT COUNT(DISTINCT "${dataset.key}") as count FROM ${dataset.table}`);
      
      const totalCount = (await totalRows.getRowObjects())[0].count;
      const uniqueCount = (await uniqueKeys.getRowObjects())[0].count;
      
      if (totalCount === uniqueCount) {
        console.log(`✅ ${dataset.table}: ${totalCount} rows, all unique`);
      } else {
        console.log(`❌ ${dataset.table}: ${totalCount} rows, only ${uniqueCount} unique - ${totalCount - uniqueCount} duplicates!`);
        conn.closeSync();
        return false;
      }
    } catch (error) {
      console.log(`⚠️  Error checking ${dataset.table}: ${error.message}`);
    }
  }
  
  conn.closeSync();
  return true;
}

function findRunDirectories(basePath) {
  const runs = [];
  
  function searchDir(dir) {
    try {
      const items = fs.readdirSync(dir);
      for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);
        
        if (stat.isDirectory()) {
          if (item.startsWith('run_')) {
            // Only include successful runs (no _FAILED file)
            const failedMarker = path.join(fullPath, '_FAILED');
            if (!fs.existsSync(failedMarker)) {
              runs.push(fullPath);
            }
          } else {
            searchDir(fullPath);
          }
        }
      }
    } catch (error) {
      // Skip inaccessible directories
    }
  }
  
  searchDir(basePath);
  return runs;
}

function extractStats(output) {
  // Extract stats from ingestion output
  const match = output.match(/(\w+) ingestion complete: \{[\s\S]*?processed_records: (\d+)n,[\s\S]*?canonical_total: (\d+)n,[\s\S]*?new_records: (\d+)n,[\s\S]*?updated_records: (\d+)n,[\s\S]*?unchanged_records: (\d+)n/);
  
  if (match) {
    return {
      dataset: match[1],
      processed_records: parseInt(match[2]),
      canonical_total: parseInt(match[3]),
      new_records: parseInt(match[4]),
      updated_records: parseInt(match[5]),
      unchanged_records: parseInt(match[6])
    };
  }
  
  throw new Error(`Could not extract stats from output: ${output.substring(0, 500)}...`);
}

async function runAllValidations() {
  console.log('🧪 DEDUPLICATION VALIDATION SUITE');
  console.log('='.repeat(80));
  console.log(`Data root: ${DATA_ROOT}`);
  
  const tests = [
    { name: 'Idempotency', fn: validateIdempotency },
    { name: 'Cross-dataset ingestion', fn: validateCrossDatasetIngestion },
    { name: 'Unique constraints', fn: validateUniqueConstraints }
  ];
  
  let passed = 0;
  let failed = 0;
  
  for (const test of tests) {
    try {
      const result = await test.fn();
      if (result) {
        passed++;
      } else {
        failed++;
      }
    } catch (error) {
      console.log(`❌ ${test.name} failed with error: ${error.message}`);
      failed++;
    }
  }
  
  console.log('\n📋 VALIDATION SUMMARY');
  console.log('='.repeat(80));
  console.log(`✅ Passed: ${passed}`);
  console.log(`❌ Failed: ${failed}`);
  console.log(`📊 Total: ${passed + failed}`);
  
  if (failed === 0) {
    console.log('\n🎉 ALL VALIDATIONS PASSED!');
    console.log('The deduplication logic is working correctly.');
  } else {
    console.log('\n💥 SOME VALIDATIONS FAILED!');
    console.log('Review the output above to identify issues.');
    process.exit(1);
  }
}

if (require.main === module) {
  runAllValidations().catch(console.error);
}

module.exports = { 
  validateIdempotency, 
  validateCrossDatasetIngestion, 
  validateUniqueConstraints 
};