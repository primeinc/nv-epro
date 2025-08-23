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
// const crypto = require('crypto'); // Not needed anymore
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const fg = require('fast-glob');
const { getEProPOCount } = require('../tools/fetch-epro-live-count');

// Track POs and their instances
const finalRows = []; // Final output rows
const allowedDuplicates = new Map(); // PO# -> {allowedCount}
const poInstances = new Map(); // PO# -> [{row, fileDate}] - all instances of each PO
const stats = {
  totalFiles: 0,
  totalRows: 0,
  uniquePOs: 0,
  duplicatesKept: 0,
  duplicatesSkipped: 0,
  updatedPOs: 0
};

/**
 * Check if two rows are identical
 */
function rowsAreIdentical(row1, row2) {
  const keys = Object.keys(row1);
  for (const key of keys) {
    if (row1[key] !== row2[key]) return false;
  }
  return true;
}

/**
 * Load allowed duplicates from config
 */
async function loadAllowedDuplicates() {
  const configPath = path.join(__dirname, '..', 'config', 'bronze', 'validated', 'bronze_legitimate_duplicates.csv');

  try {
    const content = await fs.readFile(configPath, 'utf-8');
    const rows = csv.parse(content, {
      columns: true,
      bom: true,
      skip_empty_lines: true
    });
    
    for (const row of rows) {
      const poNumber = row['PO #'];
      const duplicateCount = parseInt(row['Duplicate Count'], 10);
      
      if (poNumber && duplicateCount > 0) {
        allowedDuplicates.set(poNumber, {
          allowedCount: duplicateCount
        });
      }
    }
    
    console.log(`📋 Loaded ${allowedDuplicates.size} POs that are allowed to have duplicates\n`);
  } catch (error) {
    console.log('⚠️  Could not load allowed duplicates config:', error.message);
    console.log('   Proceeding with standard deduplication\n');
  }
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
    const poNumber = row['PO #'];
    
    if (!poNumber) continue; // Skip rows without PO numbers
    
    // Store ALL instances of each PO
    if (!poInstances.has(poNumber)) {
      poInstances.set(poNumber, []);
    }
    
    // Add this instance (no deduplication here)
    poInstances.get(poNumber).push({
      row: row,
      fileDate: fileDate
    });
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
  console.log('\n=== [scripts/debug/raw-po-consolidator.js] ===');
  console.log('🔍 Finding all PO CSV files...\n');
  
  // Load allowed duplicates configuration
  await loadAllowedDuplicates();
  
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
  
  // Process all collected PO instances
  console.log('\n⚙️  Processing PO instances...\n');
  
  // Process PO instances to get final rows
  
  for (const [poNumber, instances] of poInstances) {
    // Check if this PO is in the allowed duplicates list
    if (allowedDuplicates.has(poNumber)) {
      const allowedInfo = allowedDuplicates.get(poNumber);
      
      // For allowed duplicates, keep the most recent N instances (not first N)
      const toKeep = Math.min(instances.length, allowedInfo.allowedCount);
      const recentInstances = instances.slice(-toKeep); // Take last N (most recent)
      for (const instance of recentInstances) {
        finalRows.push(instance.row);
        stats.duplicatesKept++;
      }
      
      if (instances.length > 0) {
        console.log(`  PO ${poNumber}: kept ${toKeep} of ${instances.length} instances (allowed: ${allowedInfo.allowedCount})`);
      }
      
      stats.duplicatesSkipped += instances.length - toKeep;
    } else {
      // Normal PO - keep the most recent instance (last in chronological order)
      const mostRecentRow = instances[instances.length - 1].row;
      
      finalRows.push(mostRecentRow);
      stats.uniquePOs++;
      
      // Skip the rest as duplicates from re-scraping
      if (instances.length > 1) {
        stats.duplicatesSkipped += instances.length - 1;
      }
    }
  }
  
  // Output deduplicated data
  // Ensure debug output directory exists
  const debugDir = 'debug';
  await fs.mkdir(debugDir, { recursive: true }).catch(() => {});  // Ignore error if exists
  const outputPath = path.join(debugDir, 'purchase_orders_deduped.csv');
  const allRows = finalRows;
  
  // Sort by sent date and PO# for stable sorting
  allRows.sort((a, b) => {
    const dateA = a['Sent Date'] || '';
    const dateB = b['Sent Date'] || '';
    
    if (!dateA && !dateB) {
      // Both empty dates, sort by PO#
      return (a['PO #'] || '').localeCompare(b['PO #'] || '');
    }
    if (!dateA) return 1;
    if (!dateB) return -1;
    
    // Convert MM/DD/YYYY to sortable format
    const [monthA, dayA, yearA] = dateA.split('/');
    const [monthB, dayB, yearB] = dateB.split('/');
    
    const dateObjA = new Date(yearA, monthA - 1, dayA);
    const dateObjB = new Date(yearB, monthB - 1, dayB);
    
    const dateDiff = dateObjA - dateObjB;
    if (dateDiff !== 0) return dateDiff;
    
    // Same date, sort by PO# for stable ordering
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
  console.log(`  Unique POs processed: ${poInstances.size.toLocaleString()}`);
  console.log(`  POs with updates: ${stats.updatedPOs.toLocaleString()}`);
  console.log(`  Duplicates skipped: ${stats.duplicatesSkipped.toLocaleString()}`);
  console.log(`  Known system duplicates kept: ${stats.duplicatesKept.toLocaleString()}`);
  console.log(`  Final output rows: ${finalRows.length.toLocaleString()}`);
  console.log();
  console.log(`✅ Deduplicated data written to: ${outputPath}`);
  console.log(`   Total unique rows: ${allRows.length.toLocaleString()}`);
  console.log('='.repeat(60));
  
  // Compare with Nevada ePro website
  console.log('\n🔍 Fetching current count from Nevada ePro website...');
  try {
    const eproTotal = await getEProPOCount();
    console.log(`\n🌐 Nevada ePro website shows: ${eproTotal.toLocaleString()} POs`);
    console.log(`📊 We found: ${finalRows.length.toLocaleString()} unique rows`);
    
    const difference = eproTotal - finalRows.length;
    if (difference > 0) {
      console.log(`❓ Missing: ${difference.toLocaleString()} POs`);
      
      // Analyze what might be missing
      console.log('\n📅 Date coverage analysis:');
      const dates = allRows.map(r => r['Sent Date']).filter(d => d);
      
      // Convert MM/DD/YYYY to sortable format
      const uniqueDates = [...new Set(dates)].sort((a, b) => {
        const [monthA, dayA, yearA] = a.split('/');
        const [monthB, dayB, yearB] = b.split('/');
        const dateA = new Date(yearA, monthA - 1, dayA);
        const dateB = new Date(yearB, monthB - 1, dayB);
        return dateA - dateB;
      });
      
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
    const validationPath = path.join(debugDir, 'purchase_orders_validation.json');
    const validation = {
      timestamp: new Date().toISOString(),
      eproWebsiteCount: eproTotal,
      ourUniqueCount: finalRows.length,
      difference: difference,
      percentageCaptured: ((finalRows.length / eproTotal) * 100).toFixed(2) + '%',
      allowedDuplicatesConfig: allowedDuplicates.size,
      duplicatesKept: stats.duplicatesKept,
      posWithUpdates: stats.updatedPOs,
      duplicatesSkipped: stats.duplicatesSkipped
    };
    await fs.writeFile(validationPath, JSON.stringify(validation, null, 2));
    console.log(`\n📋 Validation metadata saved to: ${validationPath}`);
    
  } catch (error) {
    console.log('\n⚠️  Could not fetch live count from Nevada ePro:');
    console.log(`   ${error.message}`);
    console.log(`\n📊 We found: ${finalRows.length.toLocaleString()} unique rows`);
  }
}

// Run
main().catch(console.error);