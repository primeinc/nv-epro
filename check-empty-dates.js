const csv = require('csv-parse/sync');
const fs = require('fs');

let emptyDates = 0;
let totalRows = 0;

const files = [
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112643.798Z_ae4c3b7/files/po_year_2018.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112633.776Z_ae4c3b7/files/po_year_2019.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112621.339Z_ae4c3b7/files/po_year_2020.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112609.809Z_ae4c3b7/files/po_year_2021.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112557.354Z_ae4c3b7/files/po_year_2022.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112542.877Z_ae4c3b7/files/po_year_2023.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112527.524Z_ae4c3b7/files/po_year_2024.csv',
  'data/nevada-epro/purchase_orders/raw/2025/08/23/run_20250823T112514.942Z_ae4c3b7/files/po_year_2025.csv'
];

for (const file of files) {
  const content = fs.readFileSync(file, 'utf-8');
  const rows = csv.parse(content, { columns: true, skip_empty_lines: true, bom: true });
  
  for (const row of rows) {
    totalRows++;
    if (!row['Sent Date'] || row['Sent Date'].trim() === '') {
      emptyDates++;
      console.log('Empty date in PO:', row['PO #'], 'File:', file.split('/').pop());
    }
  }
}

console.log('\nTotal rows:', totalRows);
console.log('Rows with empty dates:', emptyDates);