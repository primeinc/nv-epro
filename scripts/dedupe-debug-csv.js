#!/usr/bin/env node

/**
 * Deduplicate the debug CSV to see what we get
 */

const fs = require('fs');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const crypto = require('crypto');

console.log('📂 Reading debug CSV...');
const content = fs.readFileSync('data/po-csv-debug.csv', 'utf-8');
const allRows = csv.parse(content, {
  columns: true,
  bom: true,
  skip_empty_lines: true
});

console.log(`Total raw rows: ${allRows.length.toLocaleString()}`);

// Hash function - only hash the PO data, not the metadata
function hashPOData(row) {
  const poData = {
    'PO #': row['PO #'],
    'Description': row['Description'],
    'Vendor': row['Vendor'],
    'Organization': row['Organization'],
    'Department': row['Department'],
    'Buyer': row['Buyer'],
    'Status': row['Status'],
    'Sent Date': row['Sent Date'],
    'Total': row['Total']
  };
  const normalized = Object.keys(poData).sort().map(k => `${k}:${poData[k]}`).join('|');
  return crypto.createHash('sha256').update(normalized).digest('hex').substring(0, 16);
}

// Deduplicate by PO# + Sent Date
const uniqueByPODate = new Map();
let exactDuplicateCount = 0;
let updatedCount = 0;

allRows.forEach(row => {
  const key = `${row['PO #']}|${row['Sent Date']}`;
  const hash = hashPOData(row);
  
  if (!uniqueByPODate.has(key)) {
    // First time seeing this PO# + Date combination
    uniqueByPODate.set(key, { row, hash });
  } else {
    // Same PO# + Date exists - check if it's an exact duplicate or an update
    const existing = uniqueByPODate.get(key);
    if (existing.hash === hash) {
      // Exact duplicate - skip
      exactDuplicateCount++;
    } else {
      // Different data for same PO# + Date - update to latest
      uniqueByPODate.set(key, { row, hash });
      updatedCount++;
    }
  }
});

// Also keep the original hash-based deduplication for comparison
const uniqueRows = new Map();
const duplicateCount = new Map();

allRows.forEach(row => {
  const hash = hashPOData(row);
  
  if (!uniqueRows.has(hash)) {
    uniqueRows.set(hash, row);
    duplicateCount.set(hash, 1);
  } else {
    duplicateCount.set(hash, duplicateCount.get(hash) + 1);
  }
});

console.log(`\n📊 DEDUPLICATION RESULTS:`);
console.log('='.repeat(60));
console.log(`Raw rows: ${allRows.length.toLocaleString()}`);
console.log('');
console.log('Method 1: Hash-based (current method):');
console.log(`  Unique rows: ${uniqueRows.size.toLocaleString()}`);
console.log(`  Duplicates removed: ${(allRows.length - uniqueRows.size).toLocaleString()}`);
console.log('');
console.log('Method 2: PO# + Sent Date:');
console.log(`  Unique rows: ${uniqueByPODate.size.toLocaleString()}`);
console.log(`  Duplicates removed: ${(allRows.length - uniqueByPODate.size).toLocaleString()}`);
console.log(`  Updates applied: ${updatedCount}`);
console.log('='.repeat(60));

// Get current count from Nevada ePro
const { getEProPOCount } = require('./get-epro-po-count');

console.log('\n🌐 Comparing with Nevada ePro...');
try {
  getEProPOCount().then(eproCount => {
    console.log(`Nevada ePro shows: ${eproCount.toLocaleString()} POs`);
    console.log(`Hash-based method: ${uniqueRows.size.toLocaleString()} unique rows (${uniqueRows.size - eproCount > 0 ? '+' : ''}${uniqueRows.size - eproCount})`);
    console.log(`PO+Date method: ${uniqueByPODate.size.toLocaleString()} unique rows (${uniqueByPODate.size - eproCount > 0 ? '+' : ''}${uniqueByPODate.size - eproCount})`);
    
    if (uniqueByPODate.size === eproCount) {
      console.log(`\n✅ PO+Date method gives PERFECT MATCH!`);
    } else if (uniqueRows.size === eproCount) {
      console.log(`\n✅ Hash method gives PERFECT MATCH!`);
    } else {
      console.log(`\n❌ Neither method matches exactly`);
    }
    
    // Find the most duplicated rows
    const topDuplicates = Array.from(duplicateCount.entries())
      .filter(([hash, count]) => count > 1)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    
    if (topDuplicates.length > 0) {
      console.log('\n📋 Top 10 most duplicated rows:');
      console.log('='.repeat(60));
      topDuplicates.forEach(([hash, count], i) => {
        const row = uniqueRows.get(hash);
        console.log(`\n${i + 1}. Appears ${count} times:`);
        console.log(`   PO#: ${row['PO #']}`);
        console.log(`   Date: ${row['Sent Date']}, Status: ${row['Status']}`);
        console.log(`   Vendor: ${row['Vendor']}`);
        console.log(`   Total: ${row['Total']}`);
      });
    }
    
    // Save deduped data
    const outputPath = 'data/po-debug-deduped.csv';
    const dedupedArray = Array.from(uniqueRows.values());
    
    // Sort by sent date and PO#
    dedupedArray.sort((a, b) => {
      const dateA = a['Sent Date'] || '';
      const dateB = b['Sent Date'] || '';
      if (dateA !== dateB) return dateA.localeCompare(dateB);
      return (a['PO #'] || '').localeCompare(b['PO #'] || '');
    });
    
    const csvContent = stringify.stringify(dedupedArray, {
      header: true,
      columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total']
    });
    
    fs.writeFileSync(outputPath, csvContent);
    console.log(`\n💾 Deduped data saved to: ${outputPath}`);
    
    // Compare with epro-exact to find the differences
    console.log('\n🔍 Analyzing differences with epro-exact...');
    const eproExactContent = fs.readFileSync('data/purchase_orders_epro-exact.csv', 'utf-8');
    const eproExactRows = csv.parse(eproExactContent, {
      columns: true,
      bom: true,
      skip_empty_lines: true
    });
    
    console.log(`Nevada ePro exact file has: ${eproExactRows.length.toLocaleString()} rows`);
    
    // Create maps for both datasets
    const eproByPO = new Map();
    const eproHashes = new Set();
    eproExactRows.forEach(row => {
      const poNum = row['PO #'];
      if (!eproByPO.has(poNum)) {
        eproByPO.set(poNum, []);
      }
      eproByPO.get(poNum).push(row);
      eproHashes.add(hashPOData(row));
    });
    
    const ourByPO = new Map();
    const ourHashes = new Set();
    dedupedArray.forEach(row => {
      // Remove metadata columns before hashing
      const cleanRow = {
        'PO #': row['PO #'],
        'Description': row['Description'],
        'Vendor': row['Vendor'],
        'Organization': row['Organization'],
        'Department': row['Department'],
        'Buyer': row['Buyer'],
        'Status': row['Status'],
        'Sent Date': row['Sent Date'],
        'Total': row['Total']
      };
      const poNum = row['PO #'];
      if (!ourByPO.has(poNum)) {
        ourByPO.set(poNum, []);
      }
      ourByPO.get(poNum).push(row);
      ourHashes.add(hashPOData(cleanRow));
    });
    
    // Find differences
    const statusChanges = [];
    const trulyExtra = [];
    const inOursNotEpro = [];
    
    dedupedArray.forEach(row => {
      const hash = hashPOData(row);
      const poNum = row['PO #'];
      
      if (!eproHashes.has(hash)) {
        // This exact row doesn't exist in epro
        if (eproByPO.has(poNum)) {
          // But the PO# exists - likely a status change
          const eproVersions = eproByPO.get(poNum);
          statusChanges.push({
            ours: row,
            epro: eproVersions[0] // Show first epro version
          });
        } else {
          // PO# doesn't exist at all in epro
          trulyExtra.push(row);
        }
        inOursNotEpro.push(row);
      }
    });
    
    // Find what's in epro but not in ours - need to use clean rows without metadata
    const inEproNotOurs = [];
    const ourCleanHashes = new Set();
    dedupedArray.forEach(row => {
      // Clean the row to match epro format (no metadata columns)
      const cleanRow = {
        'PO #': row['PO #'],
        'Description': row['Description'],
        'Vendor': row['Vendor'],
        'Organization': row['Organization'],
        'Department': row['Department'],
        'Buyer': row['Buyer'],
        'Status': row['Status'],
        'Sent Date': row['Sent Date'],
        'Total': row['Total']
      };
      ourCleanHashes.add(hashPOData(cleanRow));
    });
    
    eproExactRows.forEach(row => {
      const hash = hashPOData(row);
      if (!ourCleanHashes.has(hash)) {
        inEproNotOurs.push(row);
      }
    });
    
    console.log('\n' + '='.repeat(80));
    console.log('DIFFERENCE ANALYSIS:');
    console.log('='.repeat(80));
    console.log(`Total in our data: ${dedupedArray.length}`);
    console.log(`Total in epro-exact: ${eproExactRows.length}`);
    console.log(`Net difference: ${dedupedArray.length - eproExactRows.length} (should be 18)`);
    console.log('');
    console.log(`Rows in OURS but not in EPRO (exact match): ${inOursNotEpro.length}`);
    console.log(`  - Status changes (PO exists with different data): ${statusChanges.length}`);
    console.log(`  - Truly extra POs (PO# not in epro at all): ${trulyExtra.length}`);
    console.log(`Rows in EPRO but not in OURS (exact match): ${inEproNotOurs.length}`);
    console.log('='.repeat(80));
    
    // Show status changes
    if (statusChanges.length > 0) {
      console.log('\n📝 STATUS CHANGES (same PO#, different data):');
      console.log('='.repeat(80));
      statusChanges.slice(0, 5).forEach(({ours, epro}, i) => {
        console.log(`\n${i + 1}. PO#: ${ours['PO #']}`);
        console.log(`   OURS: Status=${ours['Status']}, Date=${ours['Sent Date']}, Total=${ours['Total']}`);
        console.log(`   EPRO: Status=${epro['Status']}, Date=${epro['Sent Date']}, Total=${epro['Total']}`);
      });
      if (statusChanges.length > 5) {
        console.log(`\n... and ${statusChanges.length - 5} more status changes`);
      }
    }
    
    // Show truly extra POs
    if (trulyExtra.length > 0) {
      console.log('\n🆕 TRULY EXTRA POs (not in epro at all):');
      console.log('='.repeat(80));
      trulyExtra.forEach((row, i) => {
        console.log(`\n${i + 1}. PO#: ${row['PO #']}`);
        console.log(`   Date: ${row['Sent Date']}, Status: ${row['Status']}`);
        console.log(`   Vendor: ${row['Vendor']}`);
        console.log(`   Total: ${row['Total']}`);
      });
    }
    
    // The ACTUAL 18 extra rows calculation
    console.log('\n' + '='.repeat(80));
    console.log('THE MATH:');
    console.log('='.repeat(80));
    console.log(`We have ${inOursNotEpro.length} rows that don't exactly match epro`);
    console.log(`Epro has ${inEproNotOurs.length} rows that don't exactly match ours`);
    console.log(`Net extra rows: ${inOursNotEpro.length - inEproNotOurs.length}`);
    console.log(`This should equal: ${dedupedArray.length - eproExactRows.length}`);
    
    // Save analysis
    fs.writeFileSync('data/debug-differences.json', JSON.stringify({
      statusChanges,
      trulyExtra,
      inOursNotEpro,
      inEproNotOurs,
      summary: {
        ourTotal: dedupedArray.length,
        eproTotal: eproExactRows.length,
        netDifference: dedupedArray.length - eproExactRows.length,
        rowsInOursNotEpro: inOursNotEpro.length,
        rowsInEproNotOurs: inEproNotOurs.length,
        statusChanges: statusChanges.length,
        trulyExtra: trulyExtra.length
      }
    }, null, 2));
    console.log('\n💾 Full analysis saved to: data/debug-differences.json');
    
  }).catch(error => {
    console.log(`\n⚠️ Could not fetch Nevada ePro count: ${error.message}`);
    console.log(`We have: ${uniqueRows.size.toLocaleString()} unique rows`);
  });
} catch (error) {
  console.log(`\n⚠️ Could not fetch Nevada ePro count: ${error.message}`);
  console.log(`We have: ${uniqueRows.size.toLocaleString()} unique rows`);
}