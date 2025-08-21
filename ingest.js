#!/usr/bin/env node

// ingest.js - DuckDB-based ingestion pipeline for Nevada eProcurement data
// Uses DuckDB's auto-schema detection for robust CSV ingestion

const { DuckDBInstance } = require('@duckdb/node-api');
const fs = require('fs');
const path = require('path');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');

// Initialize DuckDB connection
let connection;

async function getConnection() {
  if (!connection) {
    // Use persistent database file to maintain state between runs
    const dbPath = path.join(DATA_ROOT, 'nevada-epro', 'canonical.duckdb');
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    const instance = await DuckDBInstance.create(dbPath);
    connection = await instance.connect();
  }
  return connection;
}

async function executeSQL(sql) {
  const conn = await getConnection();
  const result = await conn.run(sql);
  return await result.getRowObjects();
}

async function ingestDataset(runPath, dataset) {
  const conn = await getConnection();
  
  // Determine CSV pattern and canonical table name
  const patterns = {
    'purchase_orders': 'po_*.csv',
    'contracts': '*contract*.csv', 
    'bids': '*bid*.csv',
    'vendors': '*vendor*.csv'
  };
  
  const csvPattern = patterns[dataset];
  if (!csvPattern) {
    throw new Error(`Unknown dataset: ${dataset}`);
  }
  
  const csvPath = path.join(runPath, 'files', csvPattern);
  const runId = path.basename(runPath);
  
  console.log(`Ingesting ${dataset} from ${csvPath}`);
  
  // Let DuckDB auto-detect schema and load the data
  console.log('Step 1: Auto-detecting schema and loading CSV...');
  await executeSQL(`
    CREATE OR REPLACE TABLE raw_${dataset} AS
    SELECT *, 
      CURRENT_TIMESTAMP AS ingested_at,
      '${runId}' AS run_id
    FROM read_csv_auto('${csvPath.replace(/\\/g, '/')}', 
      header=true, 
      ignore_errors=false,
      sample_size=-1
    )
  `);
  
  // Show what columns we detected
  const columns = await executeSQL(`DESCRIBE raw_${dataset}`);
  console.log('Detected columns:', columns.map(c => `${c.column_name}: ${c.column_type}`));
  
  // Add content hash for change detection
  console.log('Step 2: Adding content hash...');
  await executeSQL(`
    ALTER TABLE raw_${dataset} ADD COLUMN row_hash VARCHAR;
  `);
  
  // Create hash from all original columns (excluding our added ones)
  const originalCols = columns
    .filter(c => !['ingested_at', 'run_id', 'row_hash'].includes(c.column_name))
    .map(c => `COALESCE(CAST("${c.column_name}" AS VARCHAR), '')`)
    .join(` || '|' || `);
    
  await executeSQL(`
    UPDATE raw_${dataset} 
    SET row_hash = md5(${originalCols})
  `);
  
  // Get correct primary key column based on dataset type
  const primaryKeys = {
    'purchase_orders': 'PO #',
    'contracts': 'Contract #', 
    'bids': 'Bid Solicitation #',
    'vendors': 'Vendor ID'
  };
  
  const primaryKey = primaryKeys[dataset];
  if (!primaryKey) {
    throw new Error(`Unknown dataset type: ${dataset}`);
  }
  
  // Verify primary key exists in the data
  const pkColumn = columns.find(c => c.column_name === primaryKey);
  if (!pkColumn) {
    throw new Error(`Primary key "${primaryKey}" not found in CSV. Available columns: ${columns.map(c => c.column_name).join(', ')}`);
  }
  
  console.log(`Using '${primaryKey}' as primary key`);
  
  // Create canonical table with same structure
  console.log('Step 3: Creating/updating canonical table...');
  await executeSQL(`
    CREATE TABLE IF NOT EXISTS canonical_${dataset} AS 
    SELECT *, 
      ingested_at AS first_seen_at,
      ingested_at AS last_seen_at
    FROM raw_${dataset} 
    WHERE FALSE
  `);
  
  // Upsert data using separate operations (DuckDB doesn't support MERGE)
  console.log('Step 3a: Updating existing records with content changes...');
  const updateResult = await executeSQL(`
    UPDATE canonical_${dataset}
    SET 
      row_hash = raw_${dataset}.row_hash,
      last_seen_at = raw_${dataset}.ingested_at,
      run_id = raw_${dataset}.run_id
    FROM raw_${dataset}
    WHERE canonical_${dataset}."${primaryKey}" = raw_${dataset}."${primaryKey}"
      AND canonical_${dataset}.row_hash <> raw_${dataset}.row_hash
  `);
  
  console.log('Step 3b: Inserting new records...');
  const insertResult = await executeSQL(`
    INSERT INTO canonical_${dataset}
    SELECT *, 
      ingested_at AS first_seen_at,
      ingested_at AS last_seen_at
    FROM raw_${dataset}
    WHERE "${primaryKey}" NOT IN (SELECT "${primaryKey}" FROM canonical_${dataset})
  `);
  
  console.log('Step 3c: Update last_seen_at for unchanged records...');
  const touchResult = await executeSQL(`
    UPDATE canonical_${dataset}
    SET 
      last_seen_at = raw_${dataset}.ingested_at,
      run_id = raw_${dataset}.run_id
    FROM raw_${dataset}
    WHERE canonical_${dataset}."${primaryKey}" = raw_${dataset}."${primaryKey}"
      AND canonical_${dataset}.row_hash = raw_${dataset}.row_hash
  `);
  
  // Write to Parquet
  console.log('Step 4: Writing to Parquet...');
  const canonicalPath = path.join(DATA_ROOT, 'nevada-epro', dataset, 'canonical');
  fs.mkdirSync(canonicalPath, { recursive: true });
  
  // Try to find a date column for partitioning - prioritize actual DATE columns
  const dateCol = columns.find(c => 
    (c.column_type.includes('DATE') || c.column_type.includes('TIMESTAMP')) && (
      c.column_name.toLowerCase().includes('date') || 
      c.column_name.toLowerCase().includes('sent') ||
      c.column_name.toLowerCase().includes('begin') ||
      c.column_name.toLowerCase().includes('opening')
    )
  );
  
  if (dateCol && (dateCol.column_type.includes('DATE') || dateCol.column_type.includes('TIMESTAMP'))) {
    console.log(`Partitioning by ${dateCol.column_name} (${dateCol.column_type})`);
    await executeSQL(`
      COPY (
        SELECT *,
          strftime("${dateCol.column_name}", '%Y') AS year,
          strftime("${dateCol.column_name}", '%m') AS month
        FROM canonical_${dataset}
      ) TO '${canonicalPath.replace(/\\/g, '/')}'
      WITH (FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000, OVERWRITE_OR_IGNORE true)
    `);
  } else {
    console.log(`No suitable date column found for partitioning`);
    if (dateCol) {
      console.log(`Found "${dateCol.column_name}" but type is "${dateCol.column_type}" (need DATE or TIMESTAMP)`);
    }
    await executeSQL(`
      COPY canonical_${dataset} 
      TO '${canonicalPath.replace(/\\/g, '/')}/${dataset}.parquet'
      WITH (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000, OVERWRITE_OR_IGNORE true)
    `);
  }
  
  // Get meaningful stats (what actually happened)
  const rawCount = await executeSQL(`SELECT COUNT(*) as count FROM raw_${dataset}`);
  const canonicalCount = await executeSQL(`SELECT COUNT(*) as count FROM canonical_${dataset}`);
  const newRecords = await executeSQL(`
    SELECT COUNT(*) as count FROM raw_${dataset} r
    WHERE NOT EXISTS (SELECT 1 FROM canonical_${dataset} c WHERE c."${primaryKey}" = r."${primaryKey}")
  `);
  const updatedRecords = await executeSQL(`
    SELECT COUNT(*) as count FROM raw_${dataset} r
    JOIN canonical_${dataset} c ON c."${primaryKey}" = r."${primaryKey}"
    WHERE c.row_hash <> r.row_hash
  `);
  const unchangedRecords = await executeSQL(`
    SELECT COUNT(*) as count FROM raw_${dataset} r
    JOIN canonical_${dataset} c ON c."${primaryKey}" = r."${primaryKey}"
    WHERE c.row_hash = r.row_hash
  `);
  
  const stats = {
    processed_records: rawCount[0].count,
    canonical_total: canonicalCount[0].count, 
    new_records: newRecords[0].count,
    updated_records: updatedRecords[0].count,
    unchanged_records: unchangedRecords[0].count
  };
  
  console.log(`${dataset} ingestion complete:`, stats);
  return stats;
}

async function main() {
  const args = process.argv.slice(2);
  
  if (args.length < 1) {
    console.error('Usage: node ingest.js <run_path> [dataset]');
    console.error('Example: node ingest.js data/nevada-epro/purchase_orders/raw/2025/08/21/run_xxx');
    process.exit(1);
  }
  
  const runPath = args[0];
  const dataset = args[1] || detectDataset(runPath);
  
  if (!fs.existsSync(runPath)) {
    console.error(`Run path does not exist: ${runPath}`);
    process.exit(1);
  }
  
  try {
    await ingestDataset(runPath, dataset);
  } catch (error) {
    console.error('Ingestion failed:', error);
    process.exit(1);
  } finally {
    if (connection) {
      connection.closeSync();
    }
  }
}

function detectDataset(runPath) {
  if (runPath.includes('purchase_orders')) return 'purchase_orders';
  if (runPath.includes('contracts')) return 'contracts';
  if (runPath.includes('bids')) return 'bids';
  if (runPath.includes('vendors')) return 'vendors';
  return null;
}

if (require.main === module) {
  main();
}