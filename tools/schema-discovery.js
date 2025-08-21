#!/usr/bin/env node

// Schema discovery tool - examines all CSV files to understand actual structure
const fs = require('fs');
const path = require('path');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');

async function parseCSVHeader(filePath) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    rl.on('line', (line) => {
      // Parse first line as CSV header
      const header = line.replace(/^\uFEFF/, '') // Remove BOM
        .split(',')
        .map(col => col.replace(/^"(.*)"$/, '$1')); // Remove quotes
      rl.close();
      resolve(header);
    });
    
    rl.on('error', reject);
  });
}

async function sampleCSVRows(filePath, numRows = 3) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    const rows = [];
    let lineCount = 0;
    
    rl.on('line', (line) => {
      if (lineCount <= numRows) {
        rows.push(line.replace(/^\uFEFF/, ''));
      }
      lineCount++;
      
      if (lineCount > numRows) {
        rl.close();
      }
    });
    
    rl.on('close', () => resolve(rows));
    rl.on('error', reject);
  });
}

async function analyzeCSVFile(filePath) {
  try {
    const header = await parseCSVHeader(filePath);
    const sampleRows = await sampleCSVRows(filePath, 5);
    const stats = fs.statSync(filePath);
    
    return {
      file: path.relative(DATA_ROOT, filePath),
      columns: header,
      columnCount: header.length,
      sampleRows: sampleRows.slice(1, 4), // Skip header, take 3 data rows
      fileSize: stats.size,
      modified: stats.mtime.toISOString()
    };
  } catch (error) {
    return {
      file: path.relative(DATA_ROOT, filePath),
      error: error.message
    };
  }
}

function findCSVFiles(dir, pattern = '*.csv') {
  const files = [];
  
  function searchDir(currentDir) {
    try {
      const items = fs.readdirSync(currentDir);
      
      for (const item of items) {
        const fullPath = path.join(currentDir, item);
        const stat = fs.statSync(fullPath);
        
        if (stat.isDirectory()) {
          searchDir(fullPath);
        } else if (item.toLowerCase().endsWith('.csv')) {
          files.push(fullPath);
        }
      }
    } catch (error) {
      // Skip inaccessible directories
    }
  }
  
  searchDir(dir);
  return files;
}

function groupByDataset(files) {
  const groups = {
    purchase_orders: [],
    contracts: [],
    bids: [],
    vendors: [],
    unknown: []
  };
  
  for (const file of files) {
    const fileName = path.basename(file).toLowerCase();
    const filePath = file.toLowerCase();
    
    if (fileName.includes('po_') || filePath.includes('purchase_order')) {
      groups.purchase_orders.push(file);
    } else if (fileName.includes('contract') || filePath.includes('contract')) {
      groups.contracts.push(file);
    } else if (fileName.includes('bid') || filePath.includes('bid')) {
      groups.bids.push(file);
    } else if (fileName.includes('vendor') || filePath.includes('vendor')) {
      groups.vendors.push(file);
    } else {
      groups.unknown.push(file);
    }
  }
  
  return groups;
}

async function discoverSchemas() {
  console.log('🔍 Discovering CSV schemas...\n');
  
  // Find all CSV files
  const allCSVs = findCSVFiles(DATA_ROOT);
  console.log(`Found ${allCSVs.length} CSV files total\n`);
  
  if (allCSVs.length === 0) {
    console.log('❌ No CSV files found in data directory');
    return;
  }
  
  // Group by dataset type
  const grouped = groupByDataset(allCSVs);
  
  // Analyze each group
  for (const [dataset, files] of Object.entries(grouped)) {
    if (files.length === 0) continue;
    
    console.log(`📊 ${dataset.toUpperCase()} (${files.length} files)`);
    console.log('='.repeat(60));
    
    // Analyze schemas across all files in this dataset
    const schemas = [];
    const uniqueColumns = new Set();
    
    for (const file of files.slice(0, 5)) { // Sample first 5 files
      const analysis = await analyzeCSVFile(file);
      if (!analysis.error) {
        schemas.push(analysis);
        analysis.columns.forEach(col => uniqueColumns.add(col));
      }
    }
    
    if (schemas.length > 0) {
      // Show unique column set
      const allColumns = Array.from(uniqueColumns).sort();
      console.log(`Unique columns found: ${allColumns.length}`);
      allColumns.forEach((col, i) => {
        console.log(`  ${i + 1}. "${col}"`);
      });
      
      console.log('\n📝 Sample files:');
      schemas.forEach((schema, i) => {
        console.log(`\n${i + 1}. ${schema.file}`);
        console.log(`   Columns: ${schema.columnCount}`);
        console.log(`   Size: ${Math.round(schema.fileSize / 1024)}KB`);
        console.log(`   Modified: ${schema.modified}`);
        
        if (schema.sampleRows.length > 0) {
          console.log(`   Sample row: ${schema.sampleRows[0].substring(0, 100)}...`);
        }
      });
      
      // Check for potential unique identifiers
      console.log('\n🔑 Potential unique identifiers:');
      const potentialIds = allColumns.filter(col => 
        col.toLowerCase().includes('url') ||
        col.toLowerCase().includes('id') ||
        col.toLowerCase().includes('link') ||
        col.toLowerCase().includes('guid') ||
        col.toLowerCase().includes('uuid') ||
        col.toLowerCase().includes('reference')
      );
      
      if (potentialIds.length > 0) {
        potentialIds.forEach(id => console.log(`   ✅ "${id}"`));
      } else {
        console.log('   ⚠️  No obvious unique identifier columns found');
        console.log('   🔍 Primary key candidates:');
        const candidates = allColumns.filter(col => 
          col.includes('#') || 
          col.toLowerCase().includes('number') ||
          col.toLowerCase().includes('code')
        );
        candidates.forEach(candidate => console.log(`      - "${candidate}"`));
      }
    }
    
    console.log('\n');
  }
  
  // Summary
  console.log('📋 SUMMARY');
  console.log('='.repeat(60));
  Object.entries(grouped).forEach(([dataset, files]) => {
    if (files.length > 0) {
      console.log(`${dataset}: ${files.length} files`);
    }
  });
  
  return grouped;
}

if (require.main === module) {
  discoverSchemas().catch(console.error);
}

module.exports = { discoverSchemas, analyzeCSVFile };