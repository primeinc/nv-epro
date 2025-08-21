#!/usr/bin/env node

// Analyze unique identifier patterns in actual CSV data
const fs = require('fs');
const path = require('path');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

async function analyzeCsvUniqueness(filePath, primaryKeyColumn) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    let header = null;
    const values = new Set();
    const duplicates = [];
    const missing = [];
    let rowCount = 0;
    let keyIndex = -1;
    
    rl.on('line', (line) => {
      const row = line.replace(/^\uFEFF/, '') // Remove BOM
        .split('","')
        .map(col => col.replace(/^"|"$/g, '')); // Clean quotes
      
      if (!header) {
        header = row;
        keyIndex = header.findIndex(col => col === primaryKeyColumn);
        if (keyIndex === -1) {
          reject(new Error(`Column "${primaryKeyColumn}" not found in ${filePath}`));
          return;
        }
        return;
      }
      
      rowCount++;
      const keyValue = row[keyIndex];
      
      if (!keyValue || keyValue.trim() === '') {
        missing.push(rowCount);
      } else if (values.has(keyValue)) {
        duplicates.push({ row: rowCount, value: keyValue });
      } else {
        values.add(keyValue);
      }
    });
    
    rl.on('close', () => {
      resolve({
        file: path.basename(filePath),
        primaryKey: primaryKeyColumn,
        totalRows: rowCount,
        uniqueValues: values.size,
        duplicateCount: duplicates.length,
        missingCount: missing.length,
        duplicates: duplicates.slice(0, 10), // First 10 duplicates
        missing: missing.slice(0, 10), // First 10 missing
        uniquenessRate: values.size / rowCount,
        completenessRate: (rowCount - missing.length) / rowCount
      });
    });
    
    rl.on('error', reject);
  });
}

async function analyzeAllDatasets() {
  console.log('🔍 UNIQUE IDENTIFIER ANALYSIS');
  console.log('='.repeat(80));
  
  // Define expected primary keys for each dataset
  const datasets = [
    {
      name: 'Purchase Orders',
      pattern: '**/po_*.csv',
      primaryKey: 'PO #',
      expectation: 'Should be unique per PO'
    },
    {
      name: 'Contracts', 
      pattern: '**/contract*.csv',
      primaryKey: 'Contract #',
      expectation: 'Should be unique per contract'
    },
    {
      name: 'Bids',
      pattern: '**/bid*.csv', 
      primaryKey: 'Bid Solicitation #',
      expectation: 'Should be unique per bid solicitation'
    },
    {
      name: 'Vendors',
      pattern: '**/vendor*.csv',
      primaryKey: 'Vendor ID', 
      expectation: 'Should be unique per vendor'
    }
  ];
  
  // Find CSV files
  function findCSVFiles(pattern) {
    const files = [];
    const dataDir = path.join(process.cwd(), 'data');
    
    function searchDir(dir) {
      try {
        const items = fs.readdirSync(dir);
        for (const item of items) {
          const fullPath = path.join(dir, item);
          const stat = fs.statSync(fullPath);
          
          if (stat.isDirectory()) {
            searchDir(fullPath);
          } else if (item.toLowerCase().endsWith('.csv')) {
            // Check if matches pattern
            const relativePath = path.relative(dataDir, fullPath);
            if (pattern.includes('po_') && item.includes('po_')) files.push(fullPath);
            else if (pattern.includes('contract') && item.includes('contract')) files.push(fullPath);
            else if (pattern.includes('bid') && item.includes('bid')) files.push(fullPath);
            else if (pattern.includes('vendor') && item.includes('vendor')) files.push(fullPath);
          }
        }
      } catch (error) {
        // Skip inaccessible directories
      }
    }
    
    searchDir(dataDir);
    return files;
  }
  
  for (const dataset of datasets) {
    console.log(`\n📊 ${dataset.name.toUpperCase()}`);
    console.log('-'.repeat(60));
    console.log(`Primary Key: "${dataset.primaryKey}"`);
    console.log(`Expectation: ${dataset.expectation}`);
    
    const files = findCSVFiles(dataset.pattern);
    
    if (files.length === 0) {
      console.log('❌ No CSV files found');
      continue;
    }
    
    console.log(`Found ${files.length} file(s)`);
    
    for (const file of files) {
      try {
        const analysis = await analyzeCsvUniqueness(file, dataset.primaryKey);
        
        console.log(`\n📁 ${analysis.file}`);
        console.log(`   Rows: ${analysis.totalRows.toLocaleString()}`);
        console.log(`   Unique values: ${analysis.uniqueValues.toLocaleString()}`);
        console.log(`   Duplicates: ${analysis.duplicateCount} (${(analysis.duplicateCount/analysis.totalRows*100).toFixed(1)}%)`);
        console.log(`   Missing: ${analysis.missingCount} (${(analysis.missingCount/analysis.totalRows*100).toFixed(1)}%)`);
        console.log(`   Uniqueness: ${(analysis.uniquenessRate*100).toFixed(1)}%`);
        console.log(`   Completeness: ${(analysis.completenessRate*100).toFixed(1)}%`);
        
        // Status assessment
        if (analysis.uniquenessRate >= 0.99 && analysis.completenessRate >= 0.95) {
          console.log('   ✅ EXCELLENT - Suitable as primary key');
        } else if (analysis.uniquenessRate >= 0.95 && analysis.completenessRate >= 0.90) {
          console.log('   ⚠️  GOOD - Usable with minor issues');
        } else {
          console.log('   ❌ PROBLEMATIC - Not suitable as primary key');
        }
        
        // Show examples of problems
        if (analysis.duplicates.length > 0) {
          console.log(`   🔄 Duplicate examples: ${analysis.duplicates.slice(0, 3).map(d => d.value).join(', ')}`);
        }
        
        if (analysis.missing.length > 0) {
          console.log(`   ⭕ Missing values on rows: ${analysis.missing.slice(0, 5).join(', ')}`);
        }
        
      } catch (error) {
        console.log(`   ❌ Error: ${error.message}`);
      }
    }
  }
  
  // Overall recommendations
  console.log('\n🎯 RECOMMENDATIONS');
  console.log('='.repeat(80));
  console.log('Based on analysis:');
  console.log('1. If uniqueness rate ≥ 99%: Use as primary key');
  console.log('2. If uniqueness rate < 99%: Investigate duplicates - could be amendments');
  console.log('3. If completeness rate < 95%: Add missing value handling');
  console.log('4. Consider composite keys if single field insufficient');
}

if (require.main === module) {
  analyzeAllDatasets().catch(console.error);
}

module.exports = { analyzeCsvUniqueness };