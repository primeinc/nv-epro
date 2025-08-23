#!/usr/bin/env node

const fs = require('fs');
const csv = require('csv-parse/sync');

const content = fs.readFileSync('data/purchase_orders_epro-exact.csv', 'utf-8');
const rows = csv.parse(content, { columns: true, bom: true, skip_empty_lines: true });

console.log('Total rows:', rows.length);
console.log('Target: 96,985');
console.log('Testing ALL 511 possible primary key combinations...');
console.log('='.repeat(60));

const columns = ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total'];

// Generate all possible combinations (2^9 - 1 = 511)
function getAllCombinations(arr) {
  const result = [];
  const total = Math.pow(2, arr.length);
  
  for (let i = 1; i < total; i++) {
    const combo = [];
    for (let j = 0; j < arr.length; j++) {
      if (i & (1 << j)) {
        combo.push(arr[j]);
      }
    }
    result.push(combo);
  }
  return result;
}

const allCombos = getAllCombinations(columns);
console.log(`Generated ${allCombos.length} combinations to test\n`);

const results = new Map();
const exactMatches = [];
const closeMatches = [];

// Test each combination
allCombos.forEach((combo, index) => {
  const keys = new Set();
  rows.forEach(row => {
    const key = combo.map(c => row[c] || '').join('|');
    keys.add(key);
  });
  
  const uniqueCount = keys.size;
  const diff = uniqueCount - 96985;
  
  results.set(combo.join(' + '), { count: uniqueCount, diff });
  
  if (diff === 0) {
    exactMatches.push(combo);
    console.log(`✅ EXACT MATCH: [${combo.join(' + ')}] = ${uniqueCount}`);
  } else if (Math.abs(diff) <= 5) {
    closeMatches.push({ combo, diff, count: uniqueCount });
  }
  
  // Progress indicator every 50 combinations
  if ((index + 1) % 50 === 0) {
    process.stdout.write(`Tested ${index + 1}/${allCombos.length} combinations...\r`);
  }
});

console.log(`\nTested all ${allCombos.length} combinations`);
console.log('='.repeat(60));

// Summary
console.log('\nSUMMARY:');
console.log(`Exact matches (96,985): ${exactMatches.length}`);
console.log(`Close matches (±5): ${closeMatches.length}`);

if (exactMatches.length > 0) {
  console.log('\n✅ PRIMARY KEY COMBINATIONS THAT GIVE EXACTLY 96,985:');
  exactMatches.forEach(combo => {
    console.log(`  - [${combo.join(' + ')}]`);
  });
} else {
  console.log('\n❌ NO COMBINATION OF COLUMNS GIVES EXACTLY 96,985 UNIQUE ROWS');
  console.log('\nClosest matches:');
  closeMatches.sort((a, b) => Math.abs(a.diff) - Math.abs(b.diff)).slice(0, 5).forEach(({combo, diff, count}) => {
    console.log(`  - [${combo.join(' + ')}]: ${count} (${diff > 0 ? '+' : ''}${diff})`);
  });
  
  // Find the most common unique counts
  const countFreq = new Map();
  results.forEach(({count}) => {
    countFreq.set(count, (countFreq.get(count) || 0) + 1);
  });
  
  const sortedCounts = Array.from(countFreq.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5);
  console.log('\nMost common unique counts across all combinations:');
  sortedCounts.forEach(([count, freq]) => {
    const diff = count - 96985;
    console.log(`  ${count} (${diff > 0 ? '+' : ''}${diff}): appears in ${freq} combinations`);
  });
}

// Special analysis of the 96,942 (-43) result
const combosWithMinus43 = [];
results.forEach((result, combo) => {
  if (result.diff === -43) {
    combosWithMinus43.push(combo);
  }
});

if (combosWithMinus43.length > 0) {
  console.log(`\n📊 ${combosWithMinus43.length} combinations give 96,942 (-43):`);
  console.log('This suggests 43 exact duplicate rows in the data');
  console.log('Sample combinations with this result:');
  combosWithMinus43.slice(0, 5).forEach(combo => {
    console.log(`  - [${combo}]`);
  });
}