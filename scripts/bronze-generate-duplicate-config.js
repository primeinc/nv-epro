#!/usr/bin/env node

const fs = require('fs');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');

console.log('Analyzing PO duplicates and generating config...\n');

// Read the bronze complete file (dynamically generated exact data)
const content = fs.readFileSync('config/bronze/validated/bronze_complete_with_duplicates.csv', 'utf-8');
const rows = csv.parse(content, { 
  columns: true, 
  bom: true, 
  skip_empty_lines: true 
});

// Count PO occurrences and keep first instance of each duplicate
const poCounts = {};
const poFirstInstance = {};

rows.forEach(row => {
  const po = row['PO #'];
  if (!poCounts[po]) {
    poCounts[po] = 0;
    poFirstInstance[po] = row;
  }
  poCounts[po]++;
});

// Build config rows for all POs that appear more than once
const configRows = [];
for (const [po, count] of Object.entries(poCounts)) {
  if (count > 1) {
    const row = poFirstInstance[po];
    configRows.push({
      'PO #': po,
      'Description': row['Description'],
      'Vendor': row['Vendor'],
      'Organization': row['Organization'],
      'Department': row['Department'],
      'Buyer': row['Buyer'],
      'Status': row['Status'],
      'Sent Date': row['Sent Date'],
      'Total': row['Total'],
      'Duplicate Count': count
    });
  }
}

// Sort by PO number
configRows.sort((a, b) => a['PO #'].localeCompare(b['PO #']));

// Stats
console.log(`Total rows in bronze complete: ${rows.length}`);
console.log(`Unique PO#s: ${Object.keys(poCounts).length}`);
console.log(`PO#s with duplicates: ${configRows.length}`);

// Generate CSV
const csvContent = stringify.stringify(configRows, {
  header: true,
  columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total', 'Duplicate Count']
});

// Save to config
const configPath = 'config/bronze/validated/bronze_legitimate_duplicates.csv';
fs.writeFileSync(configPath, csvContent);
console.log(`\nGenerated config with ${configRows.length} duplicate POs saved to: ${configPath}`);

// Show comparison with existing config
try {
  const existingConfig = fs.readFileSync('config/nv-epro-actual-duplicates.csv', 'utf-8');
  const existingRows = csv.parse(existingConfig, { columns: true, bom: true, skip_empty_lines: true });
  console.log(`\nExisting config has ${existingRows.length} POs`);
  console.log(`New config has ${configRows.length} POs (${configRows.length - existingRows.length} more)`);
} catch (e) {
  console.log('\nCould not compare with existing config');
}