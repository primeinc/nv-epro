#!/usr/bin/env node

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const fg = require('fast-glob');

console.log('🔧 Bronze Duplicate Validator');
console.log('============================');

async function runCommand(command, args) {
  return new Promise((resolve, reject) => {
    console.log(`\n▶️  Running: ${command} ${args.join(' ')}`);
    const child = spawn(command, args, { 
      stdio: 'inherit',
      shell: true 
    });
    
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command failed with code ${code}`));
      }
    });
    
    child.on('error', (error) => {
      reject(error);
    });
  });
}

async function findLatestFile(pattern) {
  const files = await fg(pattern);
  if (files.length === 0) {
    throw new Error(`No files found matching pattern: ${pattern}`);
  }
  
  // Sort by modification time (newest first)
  const filesWithStats = files.map(file => ({
    path: file,
    mtime: fs.statSync(file).mtime
  }));
  
  filesWithStats.sort((a, b) => b.mtime - a.mtime);
  return filesWithStats[0].path;
}

async function combineAndSortCSV(file1, file2, outputFile) {
  console.log(`\n📋 Combining CSV files...`);
  console.log(`   File 1: ${file1}`);
  console.log(`   File 2: ${file2}`);
  
  // Read both files
  const content1 = fs.readFileSync(file1, 'utf-8');
  const content2 = fs.readFileSync(file2, 'utf-8');
  
  const lines1 = content1.split('\n');
  const lines2 = content2.split('\n');
  
  // Get header from first file
  const header = lines1[0];
  
  // Combine data (skip headers)
  const allData = [];
  
  // Add data from first file
  for (let i = 1; i < lines1.length; i++) {
    if (lines1[i].trim()) {
      allData.push(lines1[i]);
    }
  }
  
  // Add data from second file
  for (let i = 1; i < lines2.length; i++) {
    if (lines2[i].trim()) {
      allData.push(lines2[i]);
    }
  }
  
  // Write combined file
  const combinedContent = header + '\n' + allData.join('\n');
  const tempFile = 'data/temp_combined.csv';
  fs.writeFileSync(tempFile, combinedContent);
  
  console.log(`   Combined ${allData.length} data rows`);
  
  // Sort the combined file
  console.log(`\n📊 Sorting combined data...`);
  await runCommand('node', ['tools/sort-csv-by-date.js', tempFile, outputFile]);
  
  // Clean up temp file
  fs.unlinkSync(tempFile);
  
  console.log(`✅ Sorted data written to: ${outputFile}`);
}

async function main() {
  try {
    // Step 1: Download first date range (01/31/2018 to 06/30/2023)
    console.log('\n📥 Step 1: Downloading first date range (01/31/2018 to 06/30/2023)...');
    await runCommand('pnpm', ['run', 'po', '01/31/2018', '06/30/2023']);
    
    // Step 2: Download second date range (07/01/2023 to 08/31/2025)
    console.log('\n📥 Step 2: Downloading second date range (07/01/2023 to 08/31/2025)...');
    await runCommand('pnpm', ['run', 'po', '07/01/2023', '08/31/2025']);
    
    // Step 3: Find the latest downloaded files
    console.log('\n🔍 Step 3: Finding latest downloaded files...');
    
    const file1Pattern = 'data/nevada-epro/purchase_orders/raw/**/po_01312018_to_06302023.csv';
    const file2Pattern = 'data/nevada-epro/purchase_orders/raw/**/po_07012023_to_08312025.csv';
    
    const file1 = await findLatestFile(file1Pattern);
    const file2 = await findLatestFile(file2Pattern);
    
    console.log(`   Found file 1: ${file1}`);
    console.log(`   Found file 2: ${file2}`);
    
    // Step 4: Combine and sort the files
    const outputDir = 'config/bronze/validated';
    const outputFile = path.join(outputDir, 'bronze_complete_with_duplicates.csv');
    
    // Ensure output directory exists
    fs.mkdirSync(outputDir, { recursive: true });
    
    await combineAndSortCSV(file1, file2, outputFile);
    
    // Step 5: Generate summary
    const finalContent = fs.readFileSync(outputFile, 'utf-8');
    const totalRows = finalContent.split('\n').filter(line => line.trim()).length - 1; // -1 for header
    
    console.log('\n🎯 VALIDATION COMPLETE');
    console.log('======================');
    console.log(`📄 Output file: ${outputFile}`);
    console.log(`📊 Total records: ${totalRows.toLocaleString()}`);
    console.log('\n✨ This file contains the complete dataset with all legitimate duplicates');
    console.log('   and can be used as the reference for bronze layer validation.');
    
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(console.error);
}