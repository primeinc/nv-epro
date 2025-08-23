#!/usr/bin/env node

/**
 * Monitor PO download ranges and alert when new ranges are needed
 * 
 * Checks:
 * - If any range is approaching the 50k export limit
 * - If the last range end date is approaching today
 * - Suggests new range configurations
 */

const fs = require('fs').promises;
const path = require('path');

const CONFIG_PATH = path.join(process.cwd(), 'config', 'po-download-ranges.json');
const EXPORT_LIMIT = 50000;
const TARGET_THRESHOLD = 49500;   // Target to maximize each range
const WARNING_THRESHOLD = 49000;  // Warn when getting close to target
const EXTENSION_DAYS = 7;         // Extend by a week at a time

/**
 * Get actual PO count from Nevada ePro website
 */
async function getLivePOCount(startDate, endDate) {
  try {
    const { getEProPOCount } = require('../tools/fetch-epro-live-count');
    const count = await getEProPOCount(startDate, endDate, false);
    return { count, source: 'live_website' };
  } catch (error) {
    console.warn(`  ⚠️  Could not fetch live count: ${error.message}`);
    
    // Fall back to Bronze data if available
    try {
      const fg = require('fast-glob');
      const bronzePattern = 'data/bronze/purchase_orders/**/data.parquet';
      const bronzeFiles = await fg(bronzePattern);
      
      if (bronzeFiles.length === 0) {
        return { count: 0, source: 'no_data' };
      }
      
      // Quick count from Bronze (less accurate but available offline)
      const duckdb = require('@duckdb/node-api');
      const conn = await duckdb.DuckDBConnection.create();
      
      try {
        const sql = `SELECT COUNT(DISTINCT "PO #") as count FROM read_parquet('${bronzeFiles[0].replace(/\\/g, '/')}')`;
        const reader = await conn.runAndReadAll(sql);
        const count = Number(reader.getRows()[0][0]);
        return { count, source: 'bronze_fallback' };
      } finally {
        conn.disconnectSync();
      }
    } catch (fallbackError) {
      return { count: 0, source: 'error' };
    }
  }
}

/**
 * Parse date string to Date object
 */
function parseDate(dateStr) {
  const [month, day, year] = dateStr.split('/');
  return new Date(year, month - 1, day);
}

/**
 * Format date for display
 */
function formatDate(date) {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = date.getFullYear();
  return `${month}/${day}/${year}`;
}

/**
 * Calculate growth rate from recent data
 */
async function calculateGrowthRate() {
  const today = new Date();
  const thirtyDaysAgo = new Date(today);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  
  const startDate = formatDate(thirtyDaysAgo);
  const endDate = formatDate(today);
  
  console.log(`   📊 Calculating growth rate...`);
  console.log(`      Period: ${startDate} to ${endDate} (30 days)`);
  
  const { count } = await getLivePOCount(startDate, endDate);
  
  if (count > 0) {
    const dailyRate = count / 30;
    const monthlyRate = dailyRate * 30;
    console.log(`      POs in period: ${count}`);
    console.log(`      Daily rate: ${dailyRate.toFixed(1)} POs/day`);
    console.log(`      Monthly rate: ${monthlyRate.toFixed(0)} POs/month`);
    return { daily: dailyRate, monthly: monthlyRate, based_on: count };
  }
  
  // Fallback to historical average
  console.log(`      ⚠️ Could not get live count, using estimate`);
  return { daily: 60, monthly: 1800, based_on: 'estimate' };
}

/**
 * Suggest extending a range that ends in the future
 */
function suggestRangeExtension(range, currentCount, growthRate, maxCount = 49500) {
  const endDate = parseDate(range.end_date);
  const today = new Date();
  
  // Only suggest extension if end date is in the future
  if (endDate <= today) {
    return null;
  }
  
  const roomLeft = maxCount - currentCount;
  if (roomLeft <= 0) {
    return null;
  }
  
  // Calculate how many more days we can add
  const daysToAdd = Math.floor(roomLeft / growthRate.daily);
  
  if (daysToAdd < 30) {
    // Not worth extending for less than a month
    return null;
  }
  
  const newEndDate = new Date(endDate);
  newEndDate.setDate(newEndDate.getDate() + daysToAdd);
  
  // Cap at 2 years from now (reasonable planning horizon)
  const twoYearsFromNow = new Date(today);
  twoYearsFromNow.setFullYear(twoYearsFromNow.getFullYear() + 2);
  
  if (newEndDate > twoYearsFromNow) {
    newEndDate.setTime(twoYearsFromNow.getTime());
  }
  
  return {
    current_end: range.end_date,
    suggested_end: formatDate(newEndDate),
    additional_capacity: roomLeft,
    days_added: Math.floor((newEndDate - endDate) / (1000 * 60 * 60 * 24))
  };
}

/**
 * Suggest a new range when current is full
 */
function suggestNextRange(lastRange, growthRate, targetCount = 49000) {
  const lastEnd = parseDate(lastRange.end_date);
  const newStart = new Date(lastEnd);
  newStart.setDate(newStart.getDate() + 1);
  
  // Calculate how many days for target count
  const daysNeeded = Math.floor(targetCount / growthRate.daily);
  
  const newEnd = new Date(newStart);
  newEnd.setDate(newEnd.getDate() + daysNeeded);
  
  return {
    id: `${formatDate(newStart).replace(/\//g, '')}_to_${formatDate(newEnd).replace(/\//g, '')}`,
    start_date: formatDate(newStart),
    end_date: formatDate(newEnd),
    estimated_count: targetCount,
    estimated_months: Math.round(daysNeeded / 30)
  };
}

/**
 * Main monitoring function
 */
async function monitorRanges(autoUpdate = false) {
  console.log('\n=== [scripts/monitor-po-ranges.js] ===');
  console.log('📊 PO Download Range Monitor\n');
  console.log('=' .repeat(60));
  
  if (autoUpdate) {
    console.log('\n🔧 AUTO-UPDATE MODE ENABLED\n');
  }
  
  // Load config
  const config = JSON.parse(await fs.readFile(CONFIG_PATH, 'utf8'));
  const originalConfig = JSON.parse(JSON.stringify(config)); // Deep copy for comparison
  const today = new Date();
  
  let hasWarnings = false;
  let hasCritical = false;
  let suggestedChanges = [];
  
  for (let i = 0; i < config.ranges.length; i++) {
    const range = config.ranges[i];
    console.log(`\n📅 Range: ${range.id}`);
    console.log(`   Period: ${range.start_date} to ${range.end_date}`);
    
    // Check actual count from live website
    const { count, source } = await getLivePOCount(range.start_date, range.end_date);
    
    if (source === 'live_website') {
      console.log(`   📊 Live count: ${count.toLocaleString()} POs`);
    } else if (source === 'bronze_fallback') {
      console.log(`   📊 Count (from Bronze): ${count.toLocaleString()} POs`);
    } else {
      console.log(`   📊 Expected: ${range.expected_count}`);
      console.log(`   ℹ️  Could not get actual count`);
    }
    
    // Only check limits and suggest changes for the LAST (active) range
    const isLastRange = i === config.ranges.length - 1;
    
    if (count > 0) {
      const percent = Math.round(count / EXPORT_LIMIT * 100);
      
      if (isLastRange) {
        // For the active range, check thresholds
        if (count >= WARNING_THRESHOLD) {
          console.warn(`   ⚠️  Approaching target (${percent}% of 50k limit)`);
          hasWarnings = true;
        } else {
          console.log(`   ✅ Within limits (${percent}% of 50k)`);
        }
      } else {
        // For historical ranges, just show the status
        console.log(`   📌 Historical range at ${percent}% of limit (frozen)`);
      }
      
      // For last range, check if we can extend or need new range
      if (isLastRange && count > 0) {
        const growthRate = await calculateGrowthRate();
        console.log(`   📈 Recent growth: ~${Math.round(growthRate.monthly).toLocaleString()} POs/month`);
        
        // Determine what action to take
        if (count < TARGET_THRESHOLD) {
          // Still room in this range
          const roomLeft = TARGET_THRESHOLD - count;
          const daysOfRoom = Math.floor(roomLeft / growthRate.daily);
          
          const endDate = parseDate(range.end_date);
          const daysUntilEnd = Math.floor((endDate - today) / (1000 * 60 * 60 * 24));
          
          console.log(`\n   📐 Capacity calculation:`);
          console.log(`      Current: ${count.toLocaleString()} POs`);
          console.log(`      Target: ${TARGET_THRESHOLD.toLocaleString()} POs`);
          console.log(`      Room left: ${roomLeft.toLocaleString()} POs`);
          console.log(`      Days of capacity: ${daysOfRoom} days (at ${growthRate.daily.toFixed(1)} POs/day)`);
          console.log(`      Days until end: ${daysUntilEnd} days`);
          
          if (daysUntilEnd <= 7) {
            // Range ending soon but not full - extend it!
            const extensionDays = Math.max(EXTENSION_DAYS, daysOfRoom);
            console.log(`\n   ⚡ ACTION NEEDED: Extend this range`);
            console.log(`      Extension logic: max(${EXTENSION_DAYS} days minimum, ${daysOfRoom} days capacity) = ${extensionDays} days`);
            
            const newEndDate = new Date(endDate);
            newEndDate.setDate(newEndDate.getDate() + extensionDays);
            
            suggestedChanges.push({
              type: 'extend',
              range_id: range.id,
              current_end: range.end_date,
              new_end: formatDate(newEndDate),
              reason: `Room for ${roomLeft.toLocaleString()} more POs`
            });
            
            console.log(`      Current end: ${range.end_date}`);
            console.log(`      Extend to: ${formatDate(newEndDate)}`);
            console.log(`      Room for ~${roomLeft.toLocaleString()} more POs`);
          } else {
            console.log(`\n   ✅ Range OK - room for ${roomLeft.toLocaleString()} more POs (~${daysOfRoom} days)`);
          }
        } else {
          // At or near capacity - need new range
          console.log(`\n   📋 At capacity - add a new range:`);
          const nextRange = suggestNextRange(range, growthRate);
          
          suggestedChanges.push({
            type: 'add',
            new_range: {
              id: nextRange.id,
              start_date: nextRange.start_date,
              end_date: nextRange.end_date,
              description: `Continuation from ${range.end_date}`,
              expected_count: `~${nextRange.estimated_count.toLocaleString()}`
            }
          });
          
          console.log(`      ID: ${nextRange.id}`);
          console.log(`      Start: ${nextRange.start_date}`);
          console.log(`      End: ${nextRange.end_date} (~${nextRange.estimated_months} months)`);
          console.log(`      Capacity: ~${nextRange.estimated_count.toLocaleString()} POs`);
        }
      }
    }
    
    // Show date status
    const endDate = parseDate(range.end_date);
    if (i === config.ranges.length - 1) {
      // Last range - show how many days left
      const daysUntilEnd = Math.floor((endDate - today) / (1000 * 60 * 60 * 24));
      if (daysUntilEnd < 0) {
        console.log(`   📅 Range ended ${Math.abs(daysUntilEnd)} days ago`);
      } else {
        console.log(`   📅 Range active for ${daysUntilEnd} more days`);
      }
    } else {
      // Historical range
      const daysAgo = Math.floor((today - endDate) / (1000 * 60 * 60 * 24));
      console.log(`   📅 Historical range (ended ${daysAgo} days ago)`);
    }
  }
  
  // Summary and auto-update
  console.log('\n' + '=' .repeat(60));
  
  if (suggestedChanges.length > 0 && autoUpdate) {
    console.log('\n🔧 APPLYING UPDATES...\n');
    
    for (const change of suggestedChanges) {
      if (change.type === 'extend') {
        // Find and update the range
        const rangeToUpdate = config.ranges.find(r => r.id === change.range_id);
        if (rangeToUpdate) {
          console.log(`   ✏️  Extending ${change.range_id} to ${change.new_end}`);
          rangeToUpdate.end_date = change.new_end;
        }
      } else if (change.type === 'add') {
        console.log(`   ➕ Adding new range: ${change.new_range.id}`);
        config.ranges.push(change.new_range);
      }
    }
    
    // Write updated config
    await fs.writeFile(CONFIG_PATH, JSON.stringify(config, null, 2));
    console.log('\n✅ Config updated successfully!');
    console.log(`   ${CONFIG_PATH}`);
  } else if (suggestedChanges.length > 0) {
    console.log('\n📝 Suggested changes:');
    for (const change of suggestedChanges) {
      if (change.type === 'extend') {
        console.log(`   - Extend ${change.range_id} to ${change.new_end}`);
      } else if (change.type === 'add') {
        console.log(`   - Add new range: ${change.new_range.id}`);
      }
    }
    console.log('\n💡 Run with --update to apply these changes automatically');
    process.exitCode = 1;
  } else {
    console.log('✅ All ranges optimally configured!');
  }
  
  // Show next steps
  console.log('\n📝 Next steps:');
  console.log('   1. Review any suggested range splits above');
  console.log('   2. Update config/po-download-ranges.json if needed');
  console.log('   3. Test new ranges with: pnpm run po START_DATE END_DATE');
  console.log('   4. Run full pipeline to verify: pnpm run pipeline');
}

// Run if called directly
if (require.main === module) {
  const args = process.argv.slice(2);
  const autoUpdate = args.includes('--update') || args.includes('-u');
  monitorRanges(autoUpdate).catch(console.error);
}

module.exports = { monitorRanges };