#!/usr/bin/env node

const fs = require('fs');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const crypto = require('crypto');

console.log('📂 Reading epro-exact CSV...');
const content = fs.readFileSync('data/purchase_orders_epro-exact.csv', 'utf-8');
const rows = csv.parse(content, { columns: true, bom: true, skip_empty_lines: true });

console.log(`Total rows: ${rows.length}`);

// Find exact duplicates by creating a hash of each row
const rowsByHash = new Map();
const allDuplicateRows = []; // Will include both original and duplicate

rows.forEach((row, index) => {
  // Create hash of all fields
  const rowStr = JSON.stringify([
    row['PO #'],
    row['Description'],
    row['Vendor'],
    row['Organization'],
    row['Department'],
    row['Buyer'],
    row['Status'],
    row['Sent Date'],
    row['Total']
  ]);
  const hash = crypto.createHash('sha256').update(rowStr).digest('hex');
  
  if (rowsByHash.has(hash)) {
    // This is a duplicate
    const firstOccurrence = rowsByHash.get(hash);
    
    // Add the original row if not already added
    if (!firstOccurrence.addedToDuplicates) {
      allDuplicateRows.push({
        ...firstOccurrence.row,
        _row_number: firstOccurrence.index + 2,
        _duplicate_group: hash.substring(0, 8),
        _is_original: true
      });
      firstOccurrence.addedToDuplicates = true;
    }
    
    // Add this duplicate
    allDuplicateRows.push({
      ...row,
      _row_number: index + 2,
      _duplicate_group: hash.substring(0, 8),
      _is_original: false
    });
  } else {
    // First time seeing this row
    rowsByHash.set(hash, { row, index, addedToDuplicates: false });
  }
});

// Count unique duplicate groups
const duplicateGroups = new Set();
let totalDuplicateInstances = 0;
allDuplicateRows.forEach(row => {
  duplicateGroups.add(row._duplicate_group);
  if (!row._is_original) totalDuplicateInstances++;
});

console.log(`\n📊 DUPLICATE ANALYSIS:`);
console.log('='.repeat(60));
console.log(`Unique rows: ${rowsByHash.size}`);
console.log(`Duplicate instances (not counting originals): ${totalDuplicateInstances}`);
console.log(`Total duplicate groups: ${duplicateGroups.size}`);
console.log(`Total rows involved in duplication: ${allDuplicateRows.length}`);
console.log(`Total: ${rowsByHash.size + totalDuplicateInstances} (should be ${rows.length})`);
console.log('='.repeat(60));

// Show some examples grouped
console.log('\nFirst 3 duplicate groups:');
const groupsShown = new Set();
allDuplicateRows.forEach((row) => {
  if (groupsShown.size >= 3) return;
  if (!groupsShown.has(row._duplicate_group)) {
    groupsShown.add(row._duplicate_group);
    const groupRows = allDuplicateRows.filter(r => r._duplicate_group === row._duplicate_group);
    console.log(`\nGroup ${row._duplicate_group}: ${groupRows.length} identical rows`);
    console.log(`   PO#: ${row['PO #']}`);
    console.log(`   Date: ${row['Sent Date']}, Status: ${row['Status']}`);
    console.log(`   Vendor: ${row['Vendor']}`);
    console.log(`   Total: ${row['Total']}`);
    console.log(`   Row numbers: ${groupRows.map(r => r._row_number).join(', ')}`);
  }
});

// Save all duplicates (including originals) to a CSV
const outputPath = 'data/epro-exact-all-duplicates.csv';
const csvContent = stringify.stringify(allDuplicateRows, {
  header: true,
  columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total', '_row_number', '_duplicate_group', '_is_original']
});

fs.writeFileSync(outputPath, csvContent);
console.log(`\n💾 All ${allDuplicateRows.length} rows (originals + duplicates) saved to: ${outputPath}`);

// Also save just the unique rows for comparison
const uniqueRows = Array.from(rowsByHash.values()).map(v => v.row);
const uniqueOutputPath = 'data/epro-exact-unique-only.csv';
const uniqueCsvContent = stringify.stringify(uniqueRows, {
  header: true,
  columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total']
});

fs.writeFileSync(uniqueOutputPath, uniqueCsvContent);
console.log(`💾 ${uniqueRows.length} unique rows saved to: ${uniqueOutputPath}`);