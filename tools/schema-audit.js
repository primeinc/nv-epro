#!/usr/bin/env node

// Comprehensive schema audit - what we assume vs what we actually get
const fs = require('fs');
const path = require('path');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

async function parseCSVHeaders(filePath) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    rl.on('line', (line) => {
      const header = line.replace(/^\uFEFF/, '') // Remove BOM
        .split('","')
        .map(col => col.replace(/^"|"$/g, '')); // Clean quotes
      rl.close();
      resolve(header);
    });
    
    rl.on('error', reject);
  });
}

async function sampleCSVData(filePath, numSamples = 5) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    const rows = [];
    let lineCount = 0;
    
    rl.on('line', (line) => {
      if (lineCount <= numSamples) {
        const row = line.replace(/^\uFEFF/, '')
          .split('","')
          .map(col => col.replace(/^"|"$/g, ''));
        rows.push(row);
      }
      lineCount++;
      
      if (lineCount > numSamples) {
        rl.close();
      }
    });
    
    rl.on('close', () => resolve(rows));
    rl.on('error', reject);
  });
}

async function auditDatasetSchema(dataset, csvFiles) {
  console.log(`\n📊 ${dataset.toUpperCase()} SCHEMA AUDIT`);
  console.log('='.repeat(70));
  
  if (csvFiles.length === 0) {
    console.log('❌ No CSV files found');
    return null;
  }
  
  // Get primary key expectation
  const expectedKeys = {
    'purchase_orders': 'PO #',
    'contracts': 'Contract #',
    'bids': 'Bid Solicitation #', 
    'vendors': 'Vendor ID'
  };
  
  const expectedPrimaryKey = expectedKeys[dataset];
  console.log(`Expected primary key: "${expectedPrimaryKey}"`);
  
  // Analyze first CSV file as representative
  const csvFile = csvFiles[0];
  console.log(`Analyzing: ${path.basename(csvFile)}`);
  
  try {
    const headers = await parseCSVHeaders(csvFile);
    const sampleData = await sampleCSVData(csvFile, 3);
    
    console.log(`\n📋 ACTUAL COLUMNS (${headers.length} total):`);
    headers.forEach((col, i) => {
      console.log(`  ${i + 1}. "${col}"`);
    });
    
    // Check for expected primary key
    const hasPrimaryKey = headers.includes(expectedPrimaryKey);
    console.log(`\n🔑 PRIMARY KEY CHECK:`);
    if (hasPrimaryKey) {
      console.log(`  ✅ Found expected primary key: "${expectedPrimaryKey}"`);
    } else {
      console.log(`  ❌ Missing expected primary key: "${expectedPrimaryKey}"`);
      console.log(`  🔍 Possible alternatives:`);
      const alternatives = headers.filter(h => 
        h.toLowerCase().includes('id') || 
        h.includes('#') || 
        h.toLowerCase().includes('number')
      );
      alternatives.forEach(alt => console.log(`    - "${alt}"`));
    }
    
    // Check for date columns
    console.log(`\n📅 DATE COLUMNS:`);
    const dateColumns = headers.filter(h => 
      h.toLowerCase().includes('date') ||
      h.toLowerCase().includes('time')
    );
    
    if (dateColumns.length > 0) {
      dateColumns.forEach(col => {
        const colIndex = headers.indexOf(col);
        const sampleValues = sampleData.slice(1).map(row => row[colIndex]).filter(v => v);
        console.log(`  📆 "${col}"`);
        console.log(`    Sample values: ${sampleValues.slice(0, 3).join(', ')}`);
      });
    } else {
      console.log(`  ⚠️  No obvious date columns found`);
    }
    
    // Check for monetary columns  
    console.log(`\n💰 MONETARY COLUMNS:`);
    const moneyColumns = headers.filter(h =>
      h.toLowerCase().includes('total') ||
      h.toLowerCase().includes('amount') ||
      h.toLowerCase().includes('value') ||
      h.toLowerCase().includes('cost') ||
      h.toLowerCase().includes('price') ||
      h.toLowerCase().includes('dollars') ||
      h.toLowerCase().includes('spent')
    );
    
    if (moneyColumns.length > 0) {
      moneyColumns.forEach(col => {
        const colIndex = headers.indexOf(col);
        const sampleValues = sampleData.slice(1).map(row => row[colIndex]).filter(v => v);
        console.log(`  💵 "${col}"`);
        console.log(`    Sample values: ${sampleValues.slice(0, 3).join(', ')}`);
      });
    } else {
      console.log(`  ⚠️  No obvious monetary columns found`);
    }
    
    // Look for potential issues
    console.log(`\n⚠️  POTENTIAL ISSUES:`);
    let issueCount = 0;
    
    // Check for suspicious column names
    const suspiciousColumns = headers.filter(h => 
      h.includes('Unnamed') || 
      h.includes('Column') ||
      h.trim() === '' ||
      h.includes('?')
    );
    
    if (suspiciousColumns.length > 0) {
      console.log(`  🚨 Suspicious column names: ${suspiciousColumns.map(c => `"${c}"`).join(', ')}`);
      issueCount++;
    }
    
    // Check for very long column names (might be truncated)
    const longColumns = headers.filter(h => h.length > 50);
    if (longColumns.length > 0) {
      console.log(`  📏 Very long column names (>50 chars): ${longColumns.length}`);
      issueCount++;
    }
    
    // Check for duplicate column names
    const uniqueHeaders = new Set(headers);
    if (uniqueHeaders.size !== headers.length) {
      console.log(`  🔄 Duplicate column names detected`);
      issueCount++;
    }
    
    if (issueCount === 0) {
      console.log(`  ✅ No obvious schema issues detected`);
    }
    
    // Sample data analysis
    console.log(`\n📝 SAMPLE DATA (first 2 records):`);
    for (let i = 1; i <= Math.min(2, sampleData.length - 1); i++) {
      console.log(`\n  Record ${i}:`);
      const row = sampleData[i];
      headers.forEach((header, j) => {
        const value = row[j] || '';
        const displayValue = value.length > 30 ? value.substring(0, 30) + '...' : value;
        console.log(`    ${header}: "${displayValue}"`);
      });
    }
    
    return {
      dataset,
      file: csvFile,
      headers,
      expectedPrimaryKey,
      hasPrimaryKey,
      dateColumns,
      moneyColumns,
      issues: issueCount,
      sampleData: sampleData.slice(1, 3)
    };
    
  } catch (error) {
    console.log(`❌ Error analyzing ${csvFile}: ${error.message}`);
    return null;
  }
}

async function auditIngestionAssumptions() {
  console.log(`\n🔍 INGESTION LOGIC ASSUMPTIONS AUDIT`);
  console.log('='.repeat(70));
  
  // What our current ingest.js assumes
  const assumptions = {
    'purchase_orders': {
      primaryKey: 'PO #',
      dateColumn: 'Sent Date',
      expectedFormat: 'MM/DD/YYYY',
      moneyColumn: 'Total',
      moneyFormat: '$X,XXX.XX'
    },
    'contracts': {
      primaryKey: 'Contract #', 
      dateColumn: 'Begin Date',
      expectedFormat: 'MM/DD/YYYY',
      moneyColumn: 'Dollars Spent to Date',
      moneyFormat: '$X,XXX.XX'
    },
    'bids': {
      primaryKey: 'Bid Solicitation #',
      dateColumn: 'Bid Opening Date', 
      expectedFormat: 'MM/DD/YYYY HH:MM:SS',
      moneyColumn: null,
      moneyFormat: null
    },
    'vendors': {
      primaryKey: 'Vendor ID',
      dateColumn: null,
      expectedFormat: null,
      moneyColumn: null,
      moneyFormat: null
    }
  };
  
  return assumptions;
}

async function runSchemaAudit() {
  console.log('🔍 COMPREHENSIVE SCHEMA AUDIT');
  console.log('='.repeat(80));
  console.log(`Audit Time: ${new Date().toISOString()}`);
  
  // Find all CSV files by dataset
  const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
  const datasetPaths = {
    'purchase_orders': 'nevada-epro/purchase_orders/raw',
    'contracts': 'nevada-epro/contracts/raw',
    'bids': 'nevada-epro/bids/raw', 
    'vendors': 'nevada-epro/vendors/raw'
  };
  
  const results = {};
  
  for (const [dataset, relativePath] of Object.entries(datasetPaths)) {
    const datasetPath = path.join(DATA_ROOT, relativePath);
    
    // Find CSV files
    const csvFiles = [];
    function searchCSVs(dir) {
      try {
        const items = fs.readdirSync(dir);
        for (const item of items) {
          const fullPath = path.join(dir, item);
          const stat = fs.statSync(fullPath);
          
          if (stat.isDirectory()) {
            searchCSVs(fullPath);
          } else if (item.toLowerCase().endsWith('.csv')) {
            csvFiles.push(fullPath);
          }
        }
      } catch (error) {
        // Skip inaccessible directories
      }
    }
    
    if (fs.existsSync(datasetPath)) {
      searchCSVs(datasetPath);
    }
    
    results[dataset] = await auditDatasetSchema(dataset, csvFiles);
  }
  
  // Compare with our assumptions
  console.log(`\n🎯 ASSUMPTION VALIDATION`);
  console.log('='.repeat(70));
  
  const assumptions = await auditIngestionAssumptions();
  let assumptionFailures = 0;
  
  for (const [dataset, actual] of Object.entries(results)) {
    if (!actual) continue;
    
    const expected = assumptions[dataset];
    console.log(`\n📊 ${dataset.toUpperCase()}:`);
    
    // Check primary key assumption
    if (actual.hasPrimaryKey) {
      console.log(`  ✅ Primary key "${expected.primaryKey}" exists`);
    } else {
      console.log(`  ❌ Primary key "${expected.primaryKey}" NOT found`);
      assumptionFailures++;
    }
    
    // Check date column assumption
    if (expected.dateColumn) {
      const hasDateColumn = actual.headers.includes(expected.dateColumn);
      if (hasDateColumn) {
        console.log(`  ✅ Date column "${expected.dateColumn}" exists`);
      } else {
        console.log(`  ❌ Date column "${expected.dateColumn}" NOT found`);
        console.log(`    Available date columns: ${actual.dateColumns.join(', ')}`);
        assumptionFailures++;
      }
    } else {
      console.log(`  ➖ No date column expected`);
    }
    
    // Check money column assumption  
    if (expected.moneyColumn) {
      const hasMoneyColumn = actual.headers.includes(expected.moneyColumn);
      if (hasMoneyColumn) {
        console.log(`  ✅ Money column "${expected.moneyColumn}" exists`);
      } else {
        console.log(`  ❌ Money column "${expected.moneyColumn}" NOT found`);
        console.log(`    Available money columns: ${actual.moneyColumns.join(', ')}`);
        assumptionFailures++;
      }
    } else {
      console.log(`  ➖ No money column expected`);
    }
  }
  
  // Summary
  console.log(`\n📋 AUDIT SUMMARY`);
  console.log('='.repeat(70));
  
  const datasetsAnalyzed = Object.values(results).filter(r => r !== null).length;
  console.log(`Datasets analyzed: ${datasetsAnalyzed}/4`);
  console.log(`Assumption failures: ${assumptionFailures}`);
  
  if (assumptionFailures === 0) {
    console.log(`\n🎉 ALL SCHEMA ASSUMPTIONS VALIDATED!`);
    console.log(`Our ingestion logic matches the actual CSV structure.`);
  } else {
    console.log(`\n💥 SCHEMA ASSUMPTION FAILURES DETECTED!`);
    console.log(`Our ingestion logic may not work with the actual CSV structure.`);
    console.log(`Review the failures above and update ingest.js accordingly.`);
  }
  
  return { results, assumptionFailures };
}

if (require.main === module) {
  runSchemaAudit().catch(console.error);
}

module.exports = { runSchemaAudit, auditDatasetSchema };