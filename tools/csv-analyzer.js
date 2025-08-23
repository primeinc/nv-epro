#!/usr/bin/env node

/**
 * Dataset Analysis CLI
 * 
 * Analyzes CSV files for null rates, column types, and data quality
 * 
 * Usage:
 *   node scripts/analyze-dataset.js --file path/to/file.csv
 *   node scripts/analyze-dataset.js --dataset bids  # Uses latest raw file
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/sync');
const kleur = require('kleur');

async function analyzeCSV(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  const data = fs.readFileSync(filePath, 'utf8');
  const records = csv.parse(data, { columns: true, bom: true });

  if (records.length === 0) {
    throw new Error('No data found in CSV file');
  }

  const columns = Object.keys(records[0]);
  const nullCounts = {};
  const uniqueCounts = {};
  
  columns.forEach(col => {
    nullCounts[col] = 0;
    uniqueCounts[col] = new Set();
  });

  records.forEach(row => {
    columns.forEach(col => {
      const value = row[col];
      if (!value || value.trim() === '') {
        nullCounts[col]++;
      } else {
        uniqueCounts[col].add(value);
      }
    });
  });

  return {
    filePath,
    totalRows: records.length,
    columns: columns.map(col => ({
      name: col,
      nullCount: nullCounts[col],
      nullPercent: ((nullCounts[col] / records.length) * 100),
      uniqueValues: uniqueCounts[col].size,
      sampleValues: Array.from(uniqueCounts[col]).slice(0, 3)
    }))
  };
}

async function findLatestDatasetFile(dataset) {
  // Handle common abbreviations
  const datasetMap = {
    'po': 'purchase_orders',
    'pos': 'purchase_orders',
    'bid': 'bids',
    'contract': 'contracts',
    'vendor': 'vendors'
  };
  
  const actualDataset = datasetMap[dataset] || dataset;
  const basePath = path.join('data', 'nevada-epro', actualDataset, 'raw');
  
  if (!fs.existsSync(basePath)) {
    throw new Error(`Dataset path not found: ${basePath}`);
  }

  // Find most recent run folder
  const years = fs.readdirSync(basePath).filter(d => /^\d{4}$/.test(d)).sort().reverse();
  if (years.length === 0) throw new Error(`No data found for dataset: ${dataset}`);

  for (const year of years) {
    const months = fs.readdirSync(path.join(basePath, year)).filter(d => /^\d{2}$/.test(d)).sort().reverse();
    for (const month of months) {
      const days = fs.readdirSync(path.join(basePath, year, month)).filter(d => /^\d{2}$/.test(d)).sort().reverse();
      for (const day of days) {
        const runs = fs.readdirSync(path.join(basePath, year, month, day)).filter(d => d.startsWith('run_')).sort().reverse();
        for (const run of runs) {
          const filesDir = path.join(basePath, year, month, day, run, 'files');
          if (fs.existsSync(filesDir)) {
            const csvFiles = fs.readdirSync(filesDir).filter(f => f.endsWith('.csv'));
            if (csvFiles.length > 0) {
              return path.join(filesDir, csvFiles[0]);
            }
          }
        }
      }
    }
  }

  throw new Error(`No CSV files found for dataset: ${dataset}`);
}

function formatAnalysis(analysis) {
  console.log(kleur.cyan().bold(`Dataset Analysis: ${kleur.magenta(path.basename(analysis.filePath))}`));
  console.log(`Total ${kleur.blue('rows')}: ${kleur.magenta().bold(analysis.totalRows.toLocaleString())}`);
  console.log(`\n${kleur.cyan().bold('Column Analysis:')}`);
  console.log(kleur.gray('─'.repeat(80)));

  analysis.columns.forEach((col, index) => {
    const nullPct = col.nullPercent.toFixed(1);
    const uniquePct = ((col.uniqueValues / analysis.totalRows) * 100).toFixed(1);
    
    // Color null percentages based on severity
    let nullColor = kleur.green;
    if (col.nullPercent > 50) nullColor = kleur.red;
    else if (col.nullPercent > 10) nullColor = kleur.yellow;
    
    // Color unique percentages
    let uniqueColor = kleur.green;
    if (col.uniqueValues === analysis.totalRows) uniqueColor = kleur.cyan; // 100% unique (like primary keys)
    else if (col.uniqueValues < 10) uniqueColor = kleur.yellow; // Low cardinality
    
    console.log(`${kleur.cyan().bold(col.name)}:`);
    console.log(`  Null: ${kleur.white().bold(col.nullCount)}/${kleur.white().bold(analysis.totalRows)} (${nullColor().bold(nullPct + '%')})`);
    console.log(`  Unique: ${kleur.white().bold(col.uniqueValues)} (${uniqueColor().bold(uniquePct + '%')})`);
    if (col.sampleValues.length > 0) {
      const coloredSamples = col.sampleValues.map((sample, i) => {
        // Start with colors not already used (cyan=columns, green/yellow/red=percentages, white=numbers)
        const sampleColors = [kleur.blue, kleur.magenta, kleur.gray];
        return sampleColors[i % sampleColors.length](sample);
      });
      console.log(`  Sample: ${coloredSamples.join(', ')}`);
    }
    console.log('');
  });
}

function formatJSON(analysis) {
  console.log(JSON.stringify({
    file: analysis.filePath,
    rows: analysis.totalRows,
    columns: analysis.columns.map(col => ({
      name: col.name,
      null_count: col.nullCount,
      null_percent: parseFloat(col.nullPercent.toFixed(1)),
      unique_count: col.uniqueValues,
      unique_percent: parseFloat(((col.uniqueValues / analysis.totalRows) * 100).toFixed(1)),
      sample_values: col.sampleValues
    }))
  }, null, 2));
}

async function main() {
  const args = process.argv.slice(2);
  
  // Parse format flag
  const formatIndex = args.indexOf('--format');
  const format = formatIndex >= 0 ? args[formatIndex + 1] : 'pretty';
  
  let filePath;
  
  if (args.includes('--file')) {
    const fileIndex = args.indexOf('--file');
    filePath = args[fileIndex + 1];
  } else if (args.includes('--dataset')) {
    const datasetIndex = args.indexOf('--dataset');
    const dataset = args[datasetIndex + 1];
    filePath = await findLatestDatasetFile(dataset);
  } else {
    console.error('Usage: analyze-dataset.js --file PATH [--format json|pretty]');
    console.error('   or: analyze-dataset.js --dataset DATASET [--format json|pretty]');
    process.exit(1);
  }

  try {
    const analysis = await analyzeCSV(filePath);
    
    if (format === 'json') {
      formatJSON(analysis);
    } else {
      formatAnalysis(analysis);
    }
  } catch (error) {
    console.error('Analysis failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error(err);
    process.exit(1);
  });
}