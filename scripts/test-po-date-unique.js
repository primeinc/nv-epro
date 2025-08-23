#!/usr/bin/env node

const fs = require('fs');
const csv = require('csv-parse/sync');

console.log('📂 Reading debug CSV...');
const content = fs.readFileSync('data/po-csv-debug.csv', 'utf-8');
const allRows = csv.parse(content, {
  columns: true,
  bom: true,
  skip_empty_lines: true
});

console.log(`Total raw rows: ${allRows.length.toLocaleString()}`);

// Deduplicate by PO# + Sent Date
const uniqueByPODate = new Map();

allRows.forEach(row => {
  const key = `${row['PO #']}|${row['Sent Date']}`;
  
  if (!uniqueByPODate.has(key)) {
    uniqueByPODate.set(key, row);
  }
});

console.log(`\n📊 DEDUPLICATION BY PO# + SENT DATE:`);
console.log('='.repeat(60));
console.log(`Raw rows: ${allRows.length.toLocaleString()}`);
console.log(`Unique rows: ${uniqueByPODate.size.toLocaleString()}`);
console.log(`Duplicates removed: ${(allRows.length - uniqueByPODate.size).toLocaleString()}`);
console.log('='.repeat(60));

// Compare with Nevada ePro
const { getEProPOCount } = require('./get-epro-po-count');

console.log('\n🌐 Comparing with Nevada ePro...');
getEProPOCount().then(eproCount => {
  console.log(`Nevada ePro shows: ${eproCount.toLocaleString()} POs`);
  console.log(`We have: ${uniqueByPODate.size.toLocaleString()} unique PO+Date combinations`);
  
  const difference = uniqueByPODate.size - eproCount;
  if (difference === 0) {
    console.log(`\n✅ PERFECT MATCH!`);
  } else if (difference > 0) {
    console.log(`\n❌ We have ${difference} MORE rows than Nevada ePro`);
  } else {
    console.log(`\n❌ We have ${Math.abs(difference)} FEWER rows than Nevada ePro`);
  }
}).catch(error => {
  console.log(`\n⚠️ Could not fetch Nevada ePro count: ${error.message}`);
  console.log(`We have: ${uniqueByPODate.size.toLocaleString()} unique PO+Date combinations`);
});