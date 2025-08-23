#!/usr/bin/env node

/**
 * Deduplicate raw PO CSV files
 * - Processes files in chronological order (oldest first)
 * - Tracks rows by hash
 * - Logs differences when duplicates found
 * - Updates rows when changes detected
 */

const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const fg = require('fast-glob');
const { getEProPOCount } = require('./get-epro-po-count');

// Track unique rows by hash
const uniqueRows = new Map(); // hash -> row data
const stats = {
  totalFiles: 0,
  totalRows: 0,
  uniqueRows: 0,
  duplicateRows: 0
};

/**
 * Hash a row object
 */
function hashRow(row) {
  const normalized = Object.keys(row).sort().map(k => `${k}:${row[k]}`).join('|');
  return crypto.createHash('sha256').update(normalized).digest('hex').substring(0, 16);
}

/**
 * Process a single CSV file
 */
async function processFile(filePath, fileDate) {
  console.log(`  Processing: ${path.basename(filePath)} (${fileDate.toISOString().split('T')[0]})`);
  
  const content = await fs.readFile(filePath, 'utf-8');
  const rows = csv.parse(content, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true
  });
  
  stats.totalRows += rows.length;
  
  for (const row of rows) {
    const rowHash = hashRow(row);
    
    // Only check if we've seen this EXACT row before (by hash)
    if (uniqueRows.has(rowHash)) {
      stats.duplicateRows++;
      continue;
    }
    
    // This is a unique row - add it
    uniqueRows.set(rowHash, row);
    stats.uniqueRows++;
  }
}

/**
 * Extract date from file path
 */
function getFileDate(filePath) {
  // Try to extract from path: .../YYYY/MM/DD/run_*/files/po_*.csv
  const pathMatch = filePath.match(/(\d{4})\/(\d{2})\/(\d{2})/);
  if (pathMatch) {
    return new Date(`${pathMatch[1]}-${pathMatch[2]}-${pathMatch[3]}`);
  }
  
  // Fallback to file stats
  const stat = require('fs').statSync(filePath);
  return stat.mtime;
}

/**
 * Main processing
 */
async function main() {
  console.log('🔍 Finding all PO CSV files...\n');
  
  // Find all PO CSV files
  const files = await fg('data/nevada-epro/purchase_orders/raw/**/po_*.csv');
  
  if (files.length === 0) {
    console.error('❌ No PO CSV files found');
    process.exit(1);
  }
  
  // Sort files by date (oldest first)
  const filesWithDates = files.map(f => ({
    path: f,
    date: getFileDate(f)
  }));
  
  filesWithDates.sort((a, b) => a.date - b.date);
  
  console.log(`📂 Found ${files.length} CSV files`);
  console.log(`📅 Date range: ${filesWithDates[0].date.toISOString().split('T')[0]} to ${filesWithDates[filesWithDates.length-1].date.toISOString().split('T')[0]}\n`);
  
  console.log('⚙️  Processing files chronologically...\n');
  
  // Process each file
  for (const file of filesWithDates) {
    stats.totalFiles++;
    await processFile(file.path, file.date);
  }
  
  // Output deduplicated data
  const outputPath = 'data/purchase_orders_deduped.csv';
  const allRows = Array.from(uniqueRows.values());
  
  // Sort by sent date and PO#
  allRows.sort((a, b) => {
    const dateA = a['Sent Date'] || '';
    const dateB = b['Sent Date'] || '';
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    return (a['PO #'] || '').localeCompare(b['PO #'] || '');
  });
  
  const csvContent = stringify.stringify(allRows, {
    header: true,
    columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total']
  });
  
  await fs.writeFile(outputPath, csvContent);
  
  
  // Print summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 DEDUPLICATION SUMMARY\n');
  console.log(`  Total files processed: ${stats.totalFiles}`);
  console.log(`  Total rows processed: ${stats.totalRows.toLocaleString()}`);
  console.log(`  Unique rows found: ${uniqueRows.size.toLocaleString()}`);
  console.log(`  Duplicate rows skipped: ${stats.duplicateRows.toLocaleString()}`);
  console.log();
  console.log(`✅ Deduplicated data written to: ${outputPath}`);
  console.log(`   Total unique rows: ${allRows.length.toLocaleString()}`);
  console.log('='.repeat(60));
  
  // Compare with Nevada ePro website
  console.log('\n🔍 Fetching current count from Nevada ePro website...');
  try {
    const eproTotal = await getEProPOCount();
    console.log(`\n🌐 Nevada ePro website shows: ${eproTotal.toLocaleString()} POs`);
    console.log(`📊 We found: ${uniqueRows.size.toLocaleString()} unique rows`);
    
    const difference = eproTotal - uniqueRows.size;
    if (difference > 0) {
      console.log(`❓ Missing: ${difference.toLocaleString()} POs`);
      
      // Analyze what might be missing
      console.log('\n📅 Date coverage analysis:');
      const dates = allRows.map(r => r['Sent Date']).filter(d => d);
      const uniqueDates = [...new Set(dates)].sort();
      console.log(`   Earliest PO: ${uniqueDates[0]}`);
      console.log(`   Latest PO: ${uniqueDates[uniqueDates.length - 1]}`);
      console.log(`   Total unique dates: ${uniqueDates.length}`);
      
      // Check for POs with future dates
      const today = new Date().toISOString().split('T')[0].replace(/-/g, '/');
      const futurePOs = allRows.filter(r => {
        const poDate = r['Sent Date'];
        if (!poDate) return false;
        // Convert MM/DD/YYYY to comparable format
        const [month, day, year] = poDate.split('/');
        const dateStr = `${year}/${month}/${day}`;
        return dateStr > today.replace(/\//g, '/');
      });
      
      if (futurePOs.length > 0) {
        console.log(`   POs with future dates: ${futurePOs.length}`);
      }
    } else if (difference < 0) {
      console.log(`➕ Extra: ${Math.abs(difference).toLocaleString()} POs (we have more than the website)`);
    } else {
      console.log('✅ Perfect match!');
    }
    
    // Save validation metadata
    const validationPath = 'data/purchase_orders_validation.json';
    const validation = {
      timestamp: new Date().toISOString(),
      eproWebsiteCount: eproTotal,
      ourUniqueCount: uniqueRows.size,
      difference: difference,
      percentageCaptured: ((uniqueRows.size / eproTotal) * 100).toFixed(2) + '%'
    };
    await fs.writeFile(validationPath, JSON.stringify(validation, null, 2));
    console.log(`\n📋 Validation metadata saved to: ${validationPath}`);
    
  } catch (error) {
    console.log('\n⚠️  Could not fetch live count from Nevada ePro:');
    console.log(`   ${error.message}`);
    console.log(`\n📊 We found: ${uniqueRows.size.toLocaleString()} unique rows`);
  }
}

// Run
main().catch(console.error);