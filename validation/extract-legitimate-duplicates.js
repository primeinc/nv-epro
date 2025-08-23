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

// Check for existing config before overwriting
const configPath = 'config/bronze/validated/bronze_legitimate_duplicates.csv';
let previousCount = null;
try {
  const existingConfig = fs.readFileSync(configPath, 'utf-8');
  const existingRows = csv.parse(existingConfig, { columns: true, bom: true, skip_empty_lines: true });
  previousCount = existingRows.length;
} catch (e) {
  // No existing config - that's fine
}

// Generate CSV
const csvContent = stringify.stringify(configRows, {
  header: true,
  columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total', 'Duplicate Count']
});

// Save to config
fs.writeFileSync(configPath, csvContent);
console.log(`\nGenerated config with ${configRows.length} duplicate POs saved to: ${configPath}`);

// Show comparison if we had a previous config
if (previousCount !== null) {
  const difference = configRows.length - previousCount;
  const sign = difference >= 0 ? '+' : '';
  console.log(`\nComparison: Previous config had ${previousCount} POs, new has ${configRows.length} (${sign}${difference})`);
}