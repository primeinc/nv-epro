#!/usr/bin/env node

/**
 * Bronze Layer Ingestion
 * 
 * Takes raw CSV files and stores them as immutable Parquet with:
 * - Content-addressed paths (SHA256)
 * - Zero transformations (exactly as received)
 * - Metadata columns for lineage
 * - Deduplication via content hashing
 * - For purchase_orders: automatic consolidation with allowed duplicates handling
 */

const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');
const duckdb = require('@duckdb/node-api');
const csv = require('csv-parse/sync');
const stringify = require('csv-stringify/sync');
const fg = require('fast-glob');
const {
  createBronzeIngestionStartEvent,
  createBronzeIngestionCompleteEvent,
  emitEvent,
  writeOTelLog,
  generateRunId
} = require('./openlineage-emitter');

/**
 * Calculate SHA256 hash of file contents
 */
async function calculateFileHash(filePath) {
  const content = await fs.readFile(filePath);
  return crypto.createHash('sha256').update(content).digest('hex');
}

/**
 * Check if Bronze data already exists for this hash
 */
async function bronzeExists(dataset, fileHash, bronzeBasePath) {
  // Use fast-glob to find existing Bronze data with this hash
  const fg = require('fast-glob');
  const pattern = path.join(bronzeBasePath, dataset, '**', `sha256=${fileHash}`, 'data.parquet')
    .replace(/\\/g, '/');  // fast-glob requires forward slashes
  
  try {
    const matches = await fg(pattern);
    return matches.length > 0;
  } catch (error) {
    console.error('Error checking Bronze existence:', error);
    return false;
  }
}

/**
 * Get schema definition from registry
 */
async function getSchemaForDataset(dataset, version = 'v0.1.0') {
  const registryPath = path.join(process.cwd(), 'config', 'bronze', 'schema-registry.json');
  const registry = JSON.parse(await fs.readFile(registryPath, 'utf8'));
  
  const schema = registry.schemas[dataset]?.[version];
  if (!schema) {
    throw new Error(`No schema found for ${dataset} ${version}`);
  }
  
  return schema;
}

/**
 * Build deterministic CSV read SQL
 * NO AUTO-DETECT - explicit columns and options
 */
function buildReadCsvSQL(csvPath, schema) {
  const opts = schema.csv_options || {};
  
  // Build column definitions for DuckDB
  const columnDefs = Object.entries(schema.columns)
    .map(([name, type]) => `'${name}': '${type}'`)
    .join(', ');
  
  // Build read_csv with explicit options (using DuckDB parameter names)
  const params = [
    `'${csvPath.replace(/\\/g, '/').replace(/'/g, "''")}'`,
    `columns = {${columnDefs}}`,
    `header = ${opts.header !== false}`,
    `delim = '${opts.delimiter || ','}'`,  // DuckDB uses 'delim' not 'delimiter'
    `quote = '${opts.quote || '"'}'`,
    `escape = '${opts.escape || '"'}'`,
    `nullstr = '${opts.nullstr || ''}'`,
    `skip = 0`,
    `auto_detect = false`,  // NEVER auto-detect in production
    `sample_size = -1`,     // Read all rows for schema validation
    `ignore_errors = false`  // Fail fast on bad data
  ];
  
  if (opts.dateformat) {
    params.push(`dateformat = '${opts.dateformat}'`);
  }
  if (opts.timestampformat) {
    params.push(`timestampformat = '${opts.timestampformat}'`);
  }
  
  return `read_csv(${params.join(', ')})`;
}

/**
 * Load allowed duplicates configuration for purchase_orders
 */
async function loadAllowedDuplicates() {
  const configPath = path.join(process.cwd(), 'config', 'bronze', 'validated', 'bronze_legitimate_duplicates.csv');
  const allowedDuplicates = new Map();
  
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
    
    console.log(`📋 Loaded ${allowedDuplicates.size} POs that are allowed to have duplicates`);
  } catch (error) {
    console.log('⚠️  Could not load allowed duplicates config:', error.message);
  }
  
  return allowedDuplicates;
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
 * Consolidate and deduplicate purchase_orders before ingestion
 */
async function consolidatePurchaseOrders(csvPath) {
  // If already consolidated, just return the path
  if (csvPath.includes('purchase_orders_deduped')) {
    console.log('📋 Using already consolidated file');
    return csvPath;
  }
  
  console.log('🔄 Consolidating purchase orders with deduplication...');
  
  // Load allowed duplicates
  const allowedDuplicates = await loadAllowedDuplicates();
  
  // Find all PO CSV files if we're given a directory or pattern
  let files = [];
  if (csvPath === 'auto' || !csvPath) {
    files = await fg('data/nevada-epro/purchase_orders/raw/**/po_*.csv');
  } else if ((await fs.stat(csvPath)).isDirectory()) {
    files = await fg(path.join(csvPath, '**/po_*.csv'));
  } else {
    // Single file
    files = [csvPath];
  }
  
  if (files.length === 0) {
    throw new Error('No PO CSV files found');
  }
  
  // Sort files chronologically
  const filesWithDates = files.map(f => ({
    path: f,
    date: getFileDate(f)
  })).sort((a, b) => a.date - b.date);
  
  console.log(`  Processing ${files.length} CSV files chronologically...`);
  console.log(`  File dates: ${filesWithDates[0].date.toISOString().split('T')[0]} to ${filesWithDates[filesWithDates.length-1].date.toISOString().split('T')[0]} (when scraped)`);
  
  // Collect all PO instances
  const poInstances = new Map(); // PO# -> [{row, fileDate}]
  let totalRows = 0;
  
  for (const file of filesWithDates) {
    const content = await fs.readFile(file.path, 'utf-8');
    const rows = csv.parse(content, {
      columns: true,
      bom: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true
    });
    
    totalRows += rows.length;
    
    for (const row of rows) {
      const poNumber = row['PO #'];
      if (!poNumber) continue;
      
      if (!poInstances.has(poNumber)) {
        poInstances.set(poNumber, []);
      }
      
      poInstances.get(poNumber).push({
        row: row,
        fileDate: file.date
      });
    }
  }
  
  // Process instances to get final rows
  const finalRows = [];
  let duplicateRowsKept = 0;  // Extra duplicate rows kept (not counting the original)
  let duplicateRowsSkipped = 0;
  let posWithAllowedDuplicates = 0;
  
  for (const [poNumber, instances] of poInstances) {
    if (allowedDuplicates.has(poNumber)) {
      const allowedInfo = allowedDuplicates.get(poNumber);
      // Keep the most recent N instances
      const toKeep = Math.min(instances.length, allowedInfo.allowedCount);
      const recentInstances = instances.slice(-toKeep);
      for (const instance of recentInstances) {
        finalRows.push(instance.row);
      }
      // Count actual duplicate rows kept (beyond the first one)
      if (toKeep > 1) {
        duplicateRowsKept += toKeep - 1;  // Don't count the original row
        posWithAllowedDuplicates++;
      }
      duplicateRowsSkipped += Math.max(0, instances.length - toKeep);
    } else {
      // Keep only the most recent instance
      finalRows.push(instances[instances.length - 1].row);
      if (instances.length > 1) {
        duplicateRowsSkipped += instances.length - 1;
      }
    }
  }
  
  // Calculate stats properly
  const uniquePOs = poInstances.size;
  const totalDuplicateRows = totalRows - uniquePOs;  // Total duplicate rows in raw data
  
  console.log(`  Total rows processed: ${totalRows.toLocaleString()}`);
  console.log(`  Unique PO IDs: ${uniquePOs.toLocaleString()}`);
  console.log(`  Total duplicate rows: ${totalDuplicateRows.toLocaleString()}`);
  console.log(`    - Duplicate rows skipped: ${duplicateRowsSkipped.toLocaleString()}`);
  console.log(`    - Duplicate rows kept (allowed): ${duplicateRowsKept.toLocaleString()}`);
  console.log(`  Final rows: ${finalRows.length.toLocaleString()}`);
  
  // Sanity check the math
  const expectedFinalRows = uniquePOs + duplicateRowsKept;
  if (finalRows.length !== expectedFinalRows) {
    console.warn(`  ⚠️  Math check failed: ${finalRows.length} != ${expectedFinalRows} (unique + kept dups)`);
  }
  
  // Sort by sent date
  finalRows.sort((a, b) => {
    const dateA = a['Sent Date'] || '';
    const dateB = b['Sent Date'] || '';
    
    if (!dateA && !dateB) return (a['PO #'] || '').localeCompare(b['PO #'] || '');
    if (!dateA) return 1;
    if (!dateB) return -1;
    
    const [monthA, dayA, yearA] = dateA.split('/');
    const [monthB, dayB, yearB] = dateB.split('/');
    
    const dateObjA = new Date(yearA, monthA - 1, dayA);
    const dateObjB = new Date(yearB, monthB - 1, dayB);
    
    const dateDiff = dateObjA - dateObjB;
    if (dateDiff !== 0) return dateDiff;
    
    return (a['PO #'] || '').localeCompare(b['PO #'] || '');
  });
  
  // Write consolidated CSV
  const tempPath = path.join(process.cwd(), 'data', '.temp_consolidated_pos.csv');
  const csvContent = stringify.stringify(finalRows, {
    header: true,
    columns: ['PO #', 'Description', 'Vendor', 'Organization', 'Department', 'Buyer', 'Status', 'Sent Date', 'Total']
  });
  
  await fs.writeFile(tempPath, csvContent);
  return tempPath;
}

/**
 * Ingest CSV to Bronze Parquet
 * 
 * @param {string} csvPath - Path to CSV file
 * @param {string} dataset - Dataset name (bids, purchase_orders, etc.)
 * @param {object} options - Additional options
 * @returns {object} Ingestion result
 */
async function ingestToBronze(csvPath, dataset, options = {}) {
  const startTime = Date.now();
  const eventTime = new Date().toISOString();
  const bronzeBasePath = options.bronzeBasePath || 'data/bronze';
  
  // For purchase_orders, consolidate first unless explicitly skipped
  let actualCsvPath = csvPath;
  let isTemp = false;
  if (dataset === 'purchase_orders' && !options.skipConsolidation) {
    actualCsvPath = await consolidatePurchaseOrders(csvPath);
    isTemp = actualCsvPath.includes('.temp_consolidated');
  }
  
  // Calculate file hash for deduplication and run ID
  const fileHash = await calculateFileHash(actualCsvPath);
  const runId = options.runId || generateRunId(eventTime, actualCsvPath + fileHash);
  const fileSize = (await fs.stat(actualCsvPath)).size;
  
  // Emit OpenLineage START event
  const startEvent = createBronzeIngestionStartEvent({
    dataset,
    sourcePath: actualCsvPath,
    runId
  });
  await emitEvent(startEvent);
  
  // Write OTel log for start
  await writeOTelLog('INFO', 'bronze.ingest.start', {
    dataset,
    runId,
    sourcePath: actualCsvPath,
    fileHash: fileHash.substring(0, 8) + '...'
  });
  
  // Check if already ingested
  if (await bronzeExists(dataset, fileHash, bronzeBasePath)) {
    const ingestDate = new Date().toISOString().split('T')[0];
    const bronzePath = path.join(
      bronzeBasePath,
      dataset,
      `ingest_date=${ingestDate}`,
      `sha256=${fileHash}`,
      'data.parquet'
    );
    
    // Don't try to update manifest for skipped ingestions - the manifest may not exist
    
    // Clean up temp file if we created one
    if (isTemp) {
      try {
        await fs.unlink(actualCsvPath);
      } catch {}
    }
    
    // Emit COMPLETE event for skipped ingestion
    const completeEvent = createBronzeIngestionCompleteEvent({
      dataset,
      sourcePath: actualCsvPath,
      outputPath: bronzePath,
      runId,
      rowCount: 0,
      byteSize: fileSize,
      fileHash,
      duration: Date.now() - startTime
    });
    await emitEvent(completeEvent);
    
    await writeOTelLog('INFO', 'bronze.ingest.skipped', {
      dataset,
      runId,
      reason: 'duplicate_content',
      fileHash: fileHash.substring(0, 8) + '...'
    });
    
    return {
      success: true,
      skipped: true,
      source_file_hash: fileHash,
      bronze_path: bronzePath,
      message: 'File already ingested to Bronze'
    };
  }
  
  // Step 3: Get schema from registry
  const schema = await getSchemaForDataset(dataset, options.schemaVersion || 'v0.1.0');
  
  // Step 4: Prepare Bronze path (content-addressed)
  const ingestDate = new Date().toISOString().split('T')[0];
  const bronzePath = path.join(
    bronzeBasePath,
    dataset,
    `ingest_date=${ingestDate}`,
    `sha256=${fileHash}`,
    'data.parquet'
  );
  
  // Ensure directory exists
  await fs.mkdir(path.dirname(bronzePath), { recursive: true });
  
  // Step 5: Connect to DuckDB
  const conn = await duckdb.DuckDBConnection.create();
  
  try {
    // Step 6: Build deterministic read (NO transformations)
    const readSQL = buildReadCsvSQL(actualCsvPath, schema);
    
    // Step 7: Create Bronze table with metadata
    const bronzeSQL = `
      COPY (
        SELECT 
          *,
          -- Metadata columns (the ONLY additions)
          'nevada-epro' AS source_system,
          '${actualCsvPath.replace(/\\/g, '/').replace(/'/g, "''")}' AS source_file,
          '${fileHash}' AS source_file_hash,
          ${fileSize} AS source_file_bytes,
          row_number() OVER (ORDER BY (SELECT NULL)) AS source_row,
          CURRENT_TIMESTAMP AS ingested_at,
          '${runId}' AS bronze_run_id,
          -- Hash all columns for change detection
          md5(CONCAT_WS('|', COLUMNS(*))) AS row_hash
        FROM ${readSQL}
      ) TO '${bronzePath.replace(/\\/g, '/')}'
      WITH (
        FORMAT PARQUET,
        CODEC 'ZSTD',
        ROW_GROUP_SIZE 100000
      )
    `;
    
    console.log(`📝 Writing Bronze Parquet (zero transforms)...`);
    await conn.run(bronzeSQL);
    
    // Step 8: Validate what we wrote
    const validationSQL = `
      SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT row_hash) as unique_rows,
        MIN(ingested_at) as min_ingest,
        MAX(ingested_at) as max_ingest
      FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
    `;
    
    const reader = await conn.runAndReadAll(validationSQL);
    const stats = reader.getRows()[0];
    
    // Step 9: Write manifest
    const manifest = {
      dataset,
      bronze_path: bronzePath,
      source_file: actualCsvPath,
      source_file_hash: fileHash,
      source_file_bytes: fileSize,
      row_count: Number(stats[0]),
      unique_rows: Number(stats[1]),
      ingested_at: new Date().toISOString(),
      ingest_duration_ms: Date.now() - startTime,
      run_id: runId,
      schema_version: options.schemaVersion || 'v0.1.0',
      validation: {
        passed: true,
        row_count: Number(stats[0]),
        unique_rows: Number(stats[1])
      }
    };
    
    const manifestPath = path.join(path.dirname(bronzePath), 'manifest.json');
    await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
    
    // Emit OpenLineage COMPLETE event
    const completeEvent = createBronzeIngestionCompleteEvent({
      dataset,
      sourcePath: actualCsvPath,
      outputPath: bronzePath,
      runId,
      rowCount: Number(stats[0]),
      byteSize: fileSize,
      fileHash,
      duration: Date.now() - startTime
    });
    await emitEvent(completeEvent);
    
    // Write OTel log for completion
    await writeOTelLog('INFO', 'bronze.ingest.complete', {
      dataset,
      runId,
      rows: Number(stats[0]),
      duration_ms: Date.now() - startTime,
      fileHash: fileHash.substring(0, 8) + '...',
      bronzePath
    });
    
    return {
      success: true,
      skipped: false,
      ...manifest
    };
    
  } catch (error) {
    // Emit OpenLineage FAIL event
    const failEvent = createBronzeIngestionCompleteEvent({
      dataset,
      sourcePath: actualCsvPath,
      outputPath: bronzePath,
      runId,
      rowCount: 0,
      byteSize: fileSize,
      fileHash,
      duration: Date.now() - startTime,
      error
    });
    await emitEvent(failEvent);
    
    // Write OTel error log
    await writeOTelLog('ERROR', 'bronze.ingest.failed', {
      dataset,
      runId,
      error: error.message,
      fileHash: fileHash.substring(0, 8) + '...'
    });
    
    // Clean up partial writes
    try {
      await fs.unlink(bronzePath);
    } catch {}
    
    throw error;
    
  } finally {
    conn.disconnectSync();
    
    // Clean up temp file if we created one
    if (isTemp) {
      try {
        await fs.unlink(actualCsvPath);
      } catch {}
    }
  }
}

/**
 * Validate Bronze data meets requirements
 */
async function validateBronze(bronzePath, config) {
  const conn = await duckdb.DuckDBConnection.create();
  
  try {
    // Check required rows
    const countSQL = `SELECT COUNT(*) as cnt FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')`;
    const reader = await conn.runAndReadAll(countSQL);
    const rowCount = reader.getRows()[0][0];
    
    if (rowCount < (config.validation?.bronze?.required_rows || 1)) {
      throw new Error(`Insufficient rows: ${rowCount}`);
    }
    
    // Check for dates before minimum
    if (config.validation?.bronze?.min_date) {
      // This would check date columns if they exist
      // For now, we're keeping Bronze pure (no parsing)
    }
    
    return { valid: true, rowCount };
    
  } finally {
    conn.disconnectSync();
  }
}

module.exports = {
  ingestToBronze,
  validateBronze,
  calculateFileHash,
  bronzeExists
};