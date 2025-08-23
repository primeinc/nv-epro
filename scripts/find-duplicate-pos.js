#!/usr/bin/env node

const csv = require('csv-parse/sync');
const fs = require('fs');
const crypto = require('crypto');

// Hash function
function hashRow(row) {
  const normalized = Object.keys(row).sort().map(k => `${k}:${row[k]}`).join('|');
  return crypto.createHash('sha256').update(normalized).digest('hex').substring(0, 16);
}

// Load yearly combined data (should be 96,985 rows)
const yearlyContent = fs.readFileSync('data/purchase_orders_combined_yearly.csv', 'utf-8');
const yearlyRows = csv.parse(yearlyContent, { columns: true, skip_empty_lines: true, bom: true });

// Load deduped data (has 97,003 rows)
const dedupedContent = fs.readFileSync('data/purchase_orders_deduped.csv', 'utf-8');
const dedupedRows = csv.parse(dedupedContent, { columns: true, skip_empty_lines: true, bom: true });

console.log('Yearly combined CSV rows:', yearlyRows.length);
console.log('Deduped output CSV rows:', dedupedRows.length);
console.log('Difference:', dedupedRows.length - yearlyRows.length);

// Create hash sets
const yearlyHashes = new Map();
yearlyRows.forEach(row => {
  yearlyHashes.set(hashRow(row), row);
});

const dedupedHashes = new Map();
dedupedRows.forEach(row => {
  dedupedHashes.set(hashRow(row), row);
});

// Find rows in deduped but NOT in yearly
const extraRows = [];
for (const [hash, row] of dedupedHashes) {
  if (!yearlyHashes.has(hash)) {
    extraRows.push(row);
  }
}

console.log('\n' + '='.repeat(60));
console.log(`FOUND ${extraRows.length} EXTRA ROWS IN DEDUPED OUTPUT:`);
console.log('='.repeat(60));

// Show the extra rows
extraRows.forEach((row, i) => {
  console.log(`\n${i + 1}. PO#: ${row['PO #']}`);
  console.log(`   Date: ${row['Sent Date']}`);
  console.log(`   Vendor: ${row['Vendor']}`);
  console.log(`   Total: ${row['Total']}`);
  console.log(`   Status: ${row['Status']}`);
  console.log(`   Description: ${row['Description']}`);
});

// Save analysis
fs.writeFileSync('data/extra_rows_analysis.json', JSON.stringify(extraRows, null, 2));
console.log('\nFull analysis saved to data/extra_rows_analysis.json');

// ANALYZE THE 18 EXTRA ROWS - WHY ARE THEY DIFFERENT?
console.log('\n' + '='.repeat(60));
console.log('ANALYZING WHY WE HAVE 18 EXTRA ROWS:');
console.log('='.repeat(60));

// Find the specific PO#s that are in the extra rows
const extraPONumbers = [...new Set(extraRows.map(r => r['PO #']))];
console.log(`\nThe ${extraRows.length} extra rows involve ${extraPONumbers.length} unique PO#s`);

// For each extra PO, find if it exists in yearly data with different values
extraPONumbers.forEach(poNum => {
  console.log('\n' + '='.repeat(80));
  console.log(`PO#: ${poNum}`);
  console.log('='.repeat(80));
  
  // Find this PO in extra rows (deduped but not in yearly)
  const extraVersions = extraRows.filter(r => r['PO #'] === poNum);
  
  // Find this PO in yearly data
  const yearlyVersions = yearlyRows.filter(r => r['PO #'] === poNum);
  
  console.log(`\nFound in EXTRA rows: ${extraVersions.length} version(s)`);
  console.log(`Found in YEARLY data: ${yearlyVersions.length} version(s)`);
  
  if (extraVersions.length > 0) {
    console.log('\nEXTRA ROW VERSION(S) (in deduped but NOT in yearly with this exact data):');
    extraVersions.forEach((row, i) => {
      console.log(`\n  [${i+1}] Extra version:`);
      console.log(`      Date: ${row['Sent Date']}, Status: ${row['Status']}, Total: ${row['Total']}`);
      console.log(`      Description: ${row['Description']}`);
      console.log(`      Vendor: ${row['Vendor']}`);
    });
  }
  
  if (yearlyVersions.length > 0) {
    console.log('\nYEARLY DATA VERSION(S):');
    yearlyVersions.forEach((row, i) => {
      console.log(`\n  [${i+1}] Yearly version:`);
      console.log(`      Date: ${row['Sent Date']}, Status: ${row['Status']}, Total: ${row['Total']}`);
      console.log(`      Description: ${row['Description']}`);
      console.log(`      Vendor: ${row['Vendor']}`);
    });
  }
  
  // Compare to find differences
  if (extraVersions.length > 0 && yearlyVersions.length > 0) {
    console.log('\nDIFFERENCES:');
    const extra = extraVersions[0];
    const yearly = yearlyVersions[0];
    
    Object.keys(extra).forEach(key => {
      if (extra[key] !== yearly[key]) {
        console.log(`  ${key}: "${yearly[key]}" → "${extra[key]}"`);
      }
    });
  }
});

console.log('\n' + '='.repeat(60));
console.log('SUMMARY OF 18 EXTRA ROWS:');
console.log('='.repeat(60));
console.log(`\nThese ${extraRows.length} rows exist in our deduped data but NOT in the yearly combined data.`);
console.log('They likely came from monthly files that were scraped at different times than the yearly files.');
console.log('The differences typically involve status changes (3PS → 3PCR) between scraping runs.');