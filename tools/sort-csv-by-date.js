#!/usr/bin/env node

const fs = require('fs');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');

if (process.argv.length !== 4) {
  console.error('Usage: node sort-csv-by-date.js <input-file> <output-file>');
  process.exit(1);
}

const inputFile = process.argv[2];
const outputFile = process.argv[3];

console.log(`Reading ${inputFile}...`);

// Read CSV
const content = fs.readFileSync(inputFile, 'utf-8');
const rows = csv.parse(content, { 
  columns: true, 
  bom: true, 
  skip_empty_lines: true 
});

console.log(`Sorting ${rows.length} rows by Sent Date...`);

// Sort by Sent Date, then by PO# for stable sorting
rows.sort((a, b) => {
  const dateA = a['Sent Date'] || '';
  const dateB = b['Sent Date'] || '';
  
  if (!dateA && !dateB) {
    // Both empty dates, sort by PO#
    return (a['PO #'] || '').localeCompare(b['PO #'] || '');
  }
  if (!dateA) return 1;
  if (!dateB) return -1;
  
  // Convert MM/DD/YYYY to sortable format
  const [monthA, dayA, yearA] = dateA.split('/');
  const [monthB, dayB, yearB] = dateB.split('/');
  
  const dateObjA = new Date(yearA, monthA - 1, dayA);
  const dateObjB = new Date(yearB, monthB - 1, dayB);
  
  const dateDiff = dateObjA - dateObjB;
  if (dateDiff !== 0) return dateDiff;
  
  // Same date, sort by PO# for stable ordering
  return (a['PO #'] || '').localeCompare(b['PO #'] || '');
});

// Write sorted CSV
const csvContent = stringify.stringify(rows, {
  header: true,
  columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total']
});

fs.writeFileSync(outputFile, csvContent);

console.log(`✅ Sorted CSV written to: ${outputFile}`);
console.log(`   Earliest date: ${rows[0]['Sent Date']}`);
console.log(`   Latest date: ${rows[rows.length - 1]['Sent Date']}`);