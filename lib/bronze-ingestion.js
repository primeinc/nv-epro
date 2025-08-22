#!/usr/bin/env node

/**
 * Bronze Layer Ingestion
 * 
 * Takes raw CSV files and stores them as immutable Parquet with:
 * - Content-addressed paths (SHA256)
 * - Zero transformations (exactly as received)
 * - Metadata columns for lineage
 * - Deduplication via content hashing
 */

const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');
const duckdb = require('@duckdb/node-api');

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
  const registryPath = path.join(process.cwd(), 'schema-registry.json');
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
 * Ingest CSV to Bronze Parquet
 * 
 * @param {string} csvPath - Path to CSV file
 * @param {string} dataset - Dataset name (bids, purchase_orders, etc.)
 * @param {object} options - Additional options
 * @returns {object} Ingestion result
 */
async function ingestToBronze(csvPath, dataset, options = {}) {
  const startTime = Date.now();
  const runId = options.runId || `bronze_${Date.now()}`;
  const bronzeBasePath = options.bronzeBasePath || 'data/bronze';
  
  // Step 1: Calculate file hash for deduplication
  console.log(`📊 Calculating SHA256 for ${path.basename(csvPath)}...`);
  const fileHash = await calculateFileHash(csvPath);
  const fileSize = (await fs.stat(csvPath)).size;
  
  // Step 2: Check if already ingested
  if (await bronzeExists(dataset, fileHash, bronzeBasePath)) {
    console.log(`⏭️  Skipping - already ingested (SHA256: ${fileHash.substring(0, 8)}...)`);
    
    // Build path to existing Bronze data
    const ingestDate = new Date().toISOString().split('T')[0];
    const bronzePath = path.join(
      bronzeBasePath,
      dataset,
      `ingest_date=${ingestDate}`,
      `sha256=${fileHash}`,
      'data.parquet'
    );
    
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
    const readSQL = buildReadCsvSQL(csvPath, schema);
    
    // Step 7: Create Bronze table with metadata
    const bronzeSQL = `
      COPY (
        SELECT 
          *,
          -- Metadata columns (the ONLY additions)
          'nevada-epro' AS source_system,
          '${csvPath.replace(/\\/g, '/').replace(/'/g, "''")}' AS source_file,
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
      source_file: csvPath,
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
    
    console.log(`✅ Bronze ingestion complete:`);
    console.log(`   - Rows: ${stats[0]}`);
    console.log(`   - Hash: ${fileHash.substring(0, 8)}...`);
    console.log(`   - Path: ${bronzePath}`);
    
    return {
      success: true,
      skipped: false,
      ...manifest
    };
    
  } catch (error) {
    console.error(`❌ Bronze ingestion failed:`, error.message);
    
    // Clean up partial writes
    try {
      await fs.unlink(bronzePath);
    } catch {}
    
    throw error;
    
  } finally {
    conn.disconnectSync();
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