#!/usr/bin/env node
// tools/find-unique-key-combos.js
const fs = require('fs');
const csv = require('csv-parse/sync');

// Parse command line arguments
if (process.argv.length < 4) {
  console.log('Usage: node find-unique-key-combos.js <csv-file> <target-count>');
  console.log('Example: node find-unique-key-combos.js data.csv 96985');
  console.log('\nFinds all column combinations that produce the target number of unique rows');
  process.exit(1);
}

const inputFile = process.argv[2];
const targetCount = parseInt(process.argv[3], 10);

if (!fs.existsSync(inputFile)) {
  console.error(`Error: File not found: ${inputFile}`);
  process.exit(1);
}

if (isNaN(targetCount)) {
  console.error('Error: Target count must be a number');
  process.exit(1);
}

console.log(`Analyzing: ${inputFile}`);
console.log(`Target unique rows: ${targetCount.toLocaleString()}`);

// Read CSV
const content = fs.readFileSync(inputFile, 'utf-8');
const rows = csv.parse(content, { columns: true, bom: true, skip_empty_lines: true });

console.log(`Total rows: ${rows.length.toLocaleString()}`);

// Get column names
const columns = Object.keys(rows[0]);
console.log(`Columns: ${columns.join(', ')}`);

// Limit to reasonable number of columns to avoid exponential explosion
if (columns.length > 12) {
  console.log(`Warning: ${columns.length} columns will generate ${Math.pow(2, columns.length) - 1} combinations`);
  console.log('Consider testing specific column subsets instead');
  process.exit(1);
}

console.log(`Testing ${Math.pow(2, columns.length) - 1} possible combinations...`);
console.log('='.repeat(60));

// Generate all possible combinations
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
  const diff = uniqueCount - targetCount;
  
  results.set(combo.join(' + '), { count: uniqueCount, diff });
  
  if (diff === 0) {
    exactMatches.push(combo);
    console.log(`✅ EXACT MATCH: [${combo.join(' + ')}] = ${uniqueCount.toLocaleString()}`);
  } else if (Math.abs(diff) <= 5) {
    closeMatches.push({ combo, diff, count: uniqueCount });
  }
  
  // Progress indicator
  if ((index + 1) % 100 === 0) {
    process.stdout.write(`Tested ${index + 1}/${allCombos.length} combinations...\r`);
  }
});

console.log(`\nTested all ${allCombos.length} combinations`);
console.log('='.repeat(60));

// Summary
console.log('\nSUMMARY:');
console.log(`Exact matches (${targetCount.toLocaleString()}): ${exactMatches.length}`);
console.log(`Close matches (±5): ${closeMatches.length}`);

if (exactMatches.length > 0) {
  console.log(`\n✅ PRIMARY KEY COMBINATIONS THAT GIVE EXACTLY ${targetCount.toLocaleString()}:`);
  exactMatches.forEach(combo => {
    console.log(`  - [${combo.join(' + ')}]`);
  });
} else {
  console.log(`\n❌ NO COMBINATION OF COLUMNS GIVES EXACTLY ${targetCount.toLocaleString()} UNIQUE ROWS`);
  console.log('\nClosest matches:');
  closeMatches.sort((a, b) => Math.abs(a.diff) - Math.abs(b.diff)).slice(0, 10).forEach(({combo, diff, count}) => {
    console.log(`  - [${combo.join(' + ')}]: ${count.toLocaleString()} (${diff > 0 ? '+' : ''}${diff})`);
  });
  
  // Find the most common unique counts
  const countFreq = new Map();
  results.forEach(({count}) => {
    countFreq.set(count, (countFreq.get(count) || 0) + 1);
  });
  
  const sortedCounts = Array.from(countFreq.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5);
  console.log('\nMost common unique counts across all combinations:');
  sortedCounts.forEach(([count, freq]) => {
    const diff = count - targetCount;
    console.log(`  ${count.toLocaleString()} (${diff > 0 ? '+' : ''}${diff}): appears in ${freq} combinations`);
  });
}