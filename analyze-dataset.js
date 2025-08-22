const fs = require('fs');
const csv = require('csv-parse/sync');

const data = fs.readFileSync('data/nevada-epro/vendors/raw/2025/08/21/run_20250821T215221.918Z_0489f1b/files/vendor_all.csv', 'utf8');
const records = csv.parse(data, { columns: true, bom: true });

const columns = Object.keys(records[0]);
const nullCounts = {};
columns.forEach(col => nullCounts[col] = 0);

records.forEach(row => {
  columns.forEach(col => {
    if (!row[col] || row[col].trim() === '') {
      nullCounts[col]++;
    }
  });
});

console.log('Total rows:', records.length);
console.log('\nColumn analysis:');
columns.forEach(col => {
  const pct = ((nullCounts[col] / records.length) * 100).toFixed(1);
  console.log(`${col}: ${nullCounts[col]}/${records.length} (${pct}% null)`);
});