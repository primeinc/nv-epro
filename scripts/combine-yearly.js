#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Get all yearly CSV files from today
const yearlyFiles = [
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112643.798Z_ae4c3b7/files/po_year_2018.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112633.776Z_ae4c3b7/files/po_year_2019.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112621.339Z_ae4c3b7/files/po_year_2020.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112609.809Z_ae4c3b7/files/po_year_2021.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112557.354Z_ae4c3b7/files/po_year_2022.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112542.877Z_ae4c3b7/files/po_year_2023.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112527.524Z_ae4c3b7/files/po_year_2024.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112514.942Z_ae4c3b7/files/po_year_2025.csv'
];

let header = null;
let allData = [];

for (const file of yearlyFiles) {
  const content = fs.readFileSync(file, 'utf-8');
  const lines = content.split('\n');
  
  if (!header) {
    // First file - keep the header
    header = lines[0];
  }
  
  // Skip header line and add all data rows
  for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim()) {
      allData.push(lines[i]);
    }
  }
}

// Write combined file
const output = header + '\n' + allData.join('\n');
fs.writeFileSync('data/purchase_orders_combined_yearly.csv', output);

console.log(`Combined ${yearlyFiles.length} files`);
console.log(`Total rows: ${allData.length}`);