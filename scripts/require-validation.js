#!/usr/bin/env node

/**
 * Validation guard - ensures required validation configs exist
 * before running the consolidation pipeline
 */

const fs = require('fs');
const path = require('path');

const configPath = path.join('config', 'bronze', 'validated', 'bronze_legitimate_duplicates.csv');

if (!fs.existsSync(configPath)) {
  console.error('❌ Validation config missing!');
  console.error('Run: pnpm run validation:refresh');
  process.exit(1);
}

// Check if it's not empty
const stats = fs.statSync(configPath);
if (stats.size < 100) {
  console.error('❌ Validation config appears empty or corrupted!');
  console.error('Run: pnpm run validation:refresh');
  process.exit(1);
}

// Check age (warn if older than 30 days)
const ageInDays = (Date.now() - stats.mtimeMs) / (1000 * 60 * 60 * 24);
if (ageInDays > 30) {
  console.warn('⚠️  Validation config is ' + Math.floor(ageInDays) + ' days old');
  console.warn('   Consider running: pnpm run validation:refresh');
}

console.log('✅ Validation config present');