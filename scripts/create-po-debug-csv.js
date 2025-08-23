#!/usr/bin/env node

/**
 * Create a debug CSV with ALL raw PO data including source file info
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const fg = require('fast-glob');

// Find ALL PO CSV files in raw data
console.log('🔍 Finding all PO CSV files...');
const allPOFiles = fg.sync('data/nevada-epro/purchase_orders/raw/**/po_*.csv');

console.log(`📂 Found ${allPOFiles.length} CSV files`);

// Collect all rows with metadata
const allRowsWithMeta = [];
let totalRows = 0;

// Process each file
allPOFiles.forEach((filePath, fileIndex) => {
  console.log(`Processing ${fileIndex + 1}/${allPOFiles.length}: ${path.basename(filePath)}`);
  
  // Get file stats for creation time
  const stats = fs.statSync(filePath);
  const fileCreationTime = stats.birthtime || stats.mtime; // birthtime may not be available on all systems
  
  // Extract run ID from path
  const runMatch = filePath.match(/run_([^/\\]+)/);
  const runId = runMatch ? runMatch[1] : 'unknown';
  
  // Create a more descriptive origin
  const origin = `${runId}/${path.basename(filePath)}`;
  
  // Read and parse CSV
  const content = fs.readFileSync(filePath, 'utf-8');
  const rows = csv.parse(content, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true
  });
  
  // Add metadata to each row
  rows.forEach(row => {
    allRowsWithMeta.push({
      ...row,
      '_origin_file': origin,
      '_file_created': fileCreationTime.toISOString(),
      '_full_path': filePath
    });
    totalRows++;
  });
});

console.log(`\n📊 Total rows collected: ${totalRows.toLocaleString()}`);

// Define column order - original columns first, then metadata
const originalColumns = ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total'];
const metaColumns = ['_origin_file', '_file_created', '_full_path'];
const allColumns = [...originalColumns, ...metaColumns];

// Generate CSV
console.log('✍️  Writing debug CSV...');
const csvContent = stringify.stringify(allRowsWithMeta, {
  header: true,
  columns: allColumns
});

// Write to file
const outputPath = 'data/po-csv-debug.csv';
fs.writeFileSync(outputPath, csvContent);

// Calculate file size
const outputStats = fs.statSync(outputPath);
const sizeMB = (outputStats.size / (1024 * 1024)).toFixed(2);

console.log('\n' + '='.repeat(60));
console.log('✅ DEBUG CSV CREATED');
console.log('='.repeat(60));
console.log(`Output file: ${outputPath}`);
console.log(`Total rows: ${totalRows.toLocaleString()}`);
console.log(`File size: ${sizeMB} MB`);
console.log(`Columns: ${allColumns.join(', ')}`);
console.log('='.repeat(60));