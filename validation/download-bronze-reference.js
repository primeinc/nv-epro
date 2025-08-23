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
  await runCommand('node', [path.join(__dirname, '..', 'tools', 'sort-csv-by-date.js'), tempFile, outputFile]);
  
  // Clean up temp file
  fs.unlinkSync(tempFile);
  
  console.log(`✅ Sorted data written to: ${outputFile}`);
}

async function combineMultipleCSVFiles(files, outputFile) {
  console.log(`\n📋 Combining ${files.length} CSV files...`);
  
  let header = null;
  const allData = [];
  
  for (const file of files) {
    console.log(`   Reading: ${file}`);
    const content = fs.readFileSync(file, 'utf-8');
    const lines = content.split('\n');
    
    // Get header from first file
    if (!header && lines.length > 0) {
      header = lines[0];
    }
    
    // Add data (skip header)
    for (let i = 1; i < lines.length; i++) {
      if (lines[i].trim()) {
        allData.push(lines[i]);
      }
    }
  }
  
  // Write combined file
  const combinedContent = header + '\n' + allData.join('\n');
  const tempFile = 'data/temp_combined.csv';
  fs.writeFileSync(tempFile, combinedContent);
  
  console.log(`   Combined ${allData.length} data rows`);
  
  // Sort the combined file
  console.log(`\n📊 Sorting combined data...`);
  await runCommand('node', [path.join(__dirname, '..', 'tools', 'sort-csv-by-date.js'), tempFile, outputFile]);
  
  // Clean up temp file
  fs.unlinkSync(tempFile);
  
  console.log(`✅ Sorted data written to: ${outputFile}`);
}

async function main() {
  try {
    // Load PO download ranges from config
    const configPath = path.join(process.cwd(), 'config', 'po-download-ranges.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    
    if (!config.ranges || config.ranges.length === 0) {
      throw new Error('No ranges defined in config/po-download-ranges.json');
    }
    
    const downloadedFiles = [];
    
    // Download each range from config
    for (let i = 0; i < config.ranges.length; i++) {
      const range = config.ranges[i];
      console.log(`\n📥 Step ${i + 1}: Downloading range ${range.id} (${range.start_date} to ${range.end_date})...`);
      await runCommand('pnpm', ['run', 'po', range.start_date, range.end_date]);
      
      // Build pattern for this range's file
      const filePattern = `data/nevada-epro/purchase_orders/raw/**/po_${range.id}.csv`;
      downloadedFiles.push({
        range: range,
        pattern: filePattern
      });
    }
    
    // Find the latest downloaded files
    console.log(`\n🔍 Step ${config.ranges.length + 1}: Finding latest downloaded files...`);
    
    const filesToCombine = [];
    for (const { range, pattern } of downloadedFiles) {
    
      const file = await findLatestFile(pattern);
      console.log(`   Found ${range.id}: ${file}`);
      filesToCombine.push(file);
    }
    
    if (filesToCombine.length === 0) {
      throw new Error('No files found to combine');
    }
    
    // Combine and sort the files
    console.log(`\n📋 Step ${config.ranges.length + 2}: Combining and sorting files...`);
    const outputDir = 'config/bronze/validated';
    const outputFile = path.join(outputDir, 'bronze_complete_with_duplicates.csv');
    
    // Ensure output directory exists
    fs.mkdirSync(outputDir, { recursive: true });
    
    if (filesToCombine.length === 2) {
      // Use existing function for 2 files
      await combineAndSortCSV(filesToCombine[0], filesToCombine[1], outputFile);
    } else {
      // Handle multiple files
      await combineMultipleCSVFiles(filesToCombine, outputFile);
    }
    
    // Step 5: Generate summary
    const finalContent = fs.readFileSync(outputFile, 'utf-8');
    const totalRows = finalContent.split('\n').filter(line => line.trim()).length - 1; // -1 for header
    
    console.log('\n🎯 VALIDATION COMPLETE');
    console.log('======================');
    console.log(`📄 Output file: ${outputFile}`);
    console.log(`📊 Total records: ${totalRows.toLocaleString()}`);
    console.log('\n✨ This file contains the complete dataset with all legitimate duplicates');
    console.log('   and can be used as the reference for bronze layer validation.');
    
    // OPTIONAL: Delete the large file after processing if not needed
    // Uncomment the next line to auto-delete after legitimate duplicates are extracted
    // fs.unlinkSync(outputFile);
    
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(console.error);
}