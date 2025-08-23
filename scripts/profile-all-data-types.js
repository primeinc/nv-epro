#!/usr/bin/env node

/**
 * Profile ALL Bronze data to determine optimal data types
 * Complete analysis for lossless transformations
 */

const duckdb = require('@duckdb/node-api');
const fs = require('fs').promises;

async function analyzeColumn(conn, table, col) {
  const result = {};
  
  // Get column type first
  const typeCheck = await conn.runAndReadAll(`
    SELECT typeof("${col}") as col_type FROM ${table} LIMIT 1
  `);
  const colType = typeCheck.getRows()[0][0];
  
  // Basic stats (handle non-string types)
  let statsQuery;
  if (colType.includes('VARCHAR') || colType.includes('CHAR')) {
    statsQuery = `
      SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT "${col}") as unique_vals,
        COUNT(CASE WHEN "${col}" IS NULL THEN 1 END) as nulls,
        COUNT(CASE WHEN "${col}" = '' THEN 1 END) as empty,
        MIN(LENGTH("${col}")) as min_len,
        MAX(LENGTH("${col}")) as max_len,
        CAST(COUNT(DISTINCT "${col}") AS FLOAT) / COUNT(*) as cardinality
      FROM ${table}
    `;
  } else {
    statsQuery = `
      SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT "${col}") as unique_vals,
        COUNT(CASE WHEN "${col}" IS NULL THEN 1 END) as nulls,
        0 as empty,
        0 as min_len,
        0 as max_len,
        CAST(COUNT(DISTINCT "${col}") AS FLOAT) / COUNT(*) as cardinality
      FROM ${table}
    `;
  }
  
  const stats = await conn.runAndReadAll(statsQuery);
  
  const s = stats.getRows()[0];
  result.total = Number(s[0]);
  result.unique = Number(s[1]);
  result.nulls = Number(s[2]);
  result.empty = Number(s[3]);
  result.min_length = Number(s[4]);
  result.max_length = Number(s[5]);
  result.cardinality = Number(s[6]);
  
  // Check data patterns
  if (col.toLowerCase().includes('date') || col.toLowerCase().includes('time')) {
    // Date field
    const dateCheck = await conn.runAndReadAll(`
      SELECT 
        COUNT(CASE WHEN TRY_CAST(strptime("${col}", '%m/%d/%Y') AS DATE) IS NOT NULL THEN 1 END) as valid_dates,
        MIN(TRY_CAST(strptime("${col}", '%m/%d/%Y') AS DATE)) as min_date,
        MAX(TRY_CAST(strptime("${col}", '%m/%d/%Y') AS DATE)) as max_date
      FROM ${table}
    `);
    const d = dateCheck.getRows()[0];
    result.valid_dates = Number(d[0]);
    result.date_range = [d[1], d[2]];
    result.recommended_type = result.nulls > 0 ? 'DATE' : 'DATE NOT NULL';
  }
  else if (col.toLowerCase().includes('amount') || col.toLowerCase().includes('total') || col.toLowerCase().includes('dollars') || col.toLowerCase().includes('cost')) {
    // Money field
    const moneyCheck = await conn.runAndReadAll(`
      WITH parsed AS (
        SELECT TRY_CAST(REPLACE(REPLACE("${col}", '$', ''), ',', '') AS DECIMAL(38,4)) as amount
        FROM ${table}
      )
      SELECT 
        MIN(amount) as min_val,
        MAX(amount) as max_val,
        COUNT(CASE WHEN amount = 0 THEN 1 END) as zeros,
        COUNT(CASE WHEN amount < 0 THEN 1 END) as negatives,
        MAX(LENGTH(CAST(CAST(amount AS BIGINT) AS VARCHAR))) as max_whole_digits
      FROM parsed
    `);
    const m = moneyCheck.getRows()[0];
    result.money_range = [Number(m[0]), Number(m[1])];
    result.zeros = Number(m[2]);
    result.negatives = Number(m[3]);
    const digits = Number(m[4]);
    
    // Determine precision
    if (digits <= 7) result.recommended_type = 'DECIMAL(10,2)';
    else if (digits <= 10) result.recommended_type = 'DECIMAL(12,2)';
    else if (digits <= 13) result.recommended_type = 'DECIMAL(15,2)';
    else result.recommended_type = 'DECIMAL(18,2)';
    
    if (result.nulls === 0) result.recommended_type += ' NOT NULL';
  }
  else if (col.toLowerCase().includes(' id') || col === 'ID' || col.endsWith(' #') || col.endsWith(' No')) {
    // ID field
    result.recommended_type = `VARCHAR(${Math.min(result.max_length + 10, 64)}) PRIMARY KEY`;
  }
  else if (result.unique <= 10) {
    // Enum candidate
    const values = await conn.runAndReadAll(`
      SELECT DISTINCT "${col}" FROM ${table} ORDER BY "${col}"
    `);
    result.enum_values = values.getRows().map(r => r[0]);
    result.recommended_type = `VARCHAR(${result.max_length + 5}) CHECK ("${col}" IN (${result.enum_values.map(v => `'${v}'`).join(', ')}))`;
  }
  else if (result.cardinality < 0.01 && result.unique < 1000) {
    // Low cardinality - dimension table candidate
    result.is_dimension = true;
    result.recommended_type = `INTEGER REFERENCES dim_${col.toLowerCase().replace(/[^a-z0-9]/g, '_')}`;
  }
  else {
    // Regular text
    if (result.max_length <= 50) result.recommended_type = `VARCHAR(${result.max_length + 10})`;
    else if (result.max_length <= 255) result.recommended_type = `VARCHAR(${Math.min(result.max_length + 20, 255)})`;
    else result.recommended_type = 'TEXT';
    
    if (result.nulls === 0) result.recommended_type += ' NOT NULL';
  }
  
  return result;
}

async function profileDataset(dataset) {
  const conn = await duckdb.DuckDBConnection.create();
  
  console.log(`\n${'='.repeat(80)}`);
  console.log(`DATASET: ${dataset.toUpperCase()}`);
  console.log(`${'='.repeat(80)}`);
  
  // Get Bronze data
  const bronzePath = `data/bronze/${dataset}/*/*/data.parquet`;
  
  // Check if data exists
  try {
    const count = await conn.runAndReadAll(`
      SELECT COUNT(*) FROM read_parquet('${bronzePath}')
    `);
    console.log(`Total rows: ${count.getRows()[0][0]}`);
  } catch (e) {
    console.log(`No Bronze data found for ${dataset}`);
    conn.disconnectSync();
    return null;
  }
  
  // Get columns
  const schema = await conn.runAndReadAll(`
    DESCRIBE SELECT * FROM read_parquet('${bronzePath}') LIMIT 1
  `);
  
  const columns = schema.getRows()
    .map(r => r[0])
    .filter(c => !['source_system', 'source_file', 'source_file_hash', 'source_file_bytes', 
                  'source_row', 'ingested_at', 'bronze_run_id', 'row_hash', 
                  'ingest_date', 'sha256'].includes(c))
    .filter(c => !c.startsWith('row_hash_'));  // Remove duplicate hash columns
  
  console.log(`\nColumns: ${columns.length}`);
  
  const results = {};
  
  for (const col of columns) {
    console.log(`\n[${col}]`);
    const analysis = await analyzeColumn(conn, `read_parquet('${bronzePath}')`, col);
    
    console.log(`  Cardinality: ${analysis.unique}/${analysis.total} (${(analysis.cardinality * 100).toFixed(2)}%)`);
    console.log(`  Nulls: ${analysis.nulls}, Empty: ${analysis.empty}`);
    console.log(`  Length: ${analysis.min_length}-${analysis.max_length}`);
    
    if (analysis.enum_values) {
      console.log(`  Values: ${analysis.enum_values.join(', ')}`);
    }
    if (analysis.money_range) {
      console.log(`  Range: $${analysis.money_range[0]} to $${analysis.money_range[1]}`);
    }
    if (analysis.date_range) {
      console.log(`  Dates: ${analysis.date_range[0]} to ${analysis.date_range[1]}`);
    }
    if (analysis.is_dimension) {
      console.log(`  ** DIMENSION TABLE CANDIDATE **`);
    }
    
    console.log(`  RECOMMENDED: ${analysis.recommended_type}`);
    results[col] = analysis;
  }
  
  conn.disconnectSync();
  return results;
}

async function generateOptimalSchema() {
  const datasets = ['purchase_orders', 'contracts', 'vendors', 'bids'];
  const allSchemas = {};
  
  for (const dataset of datasets) {
    const schema = await profileDataset(dataset);
    if (schema) {
      allSchemas[dataset] = schema;
    }
  }
  
  // Save schema analysis
  const outputPath = 'data-profiling-results.json';
  await fs.writeFile(outputPath, JSON.stringify(allSchemas, null, 2));
  console.log(`\n\nSchema analysis saved to ${outputPath}`);
  
  // Generate DDL
  console.log('\n' + '='.repeat(80));
  console.log('RECOMMENDED DDL STATEMENTS');
  console.log('='.repeat(80));
  
  for (const [dataset, schema] of Object.entries(allSchemas)) {
    console.log(`\n-- ${dataset.toUpperCase()} Silver Table`);
    console.log(`CREATE TABLE silver_${dataset} (`);
    
    const cols = [];
    for (const [col, info] of Object.entries(schema)) {
      cols.push(`  ${col.toLowerCase().replace(/[^a-z0-9]/g, '_')} ${info.recommended_type}`);
    }
    console.log(cols.join(',\n'));
    console.log(');');
  }
}

if (require.main === module) {
  generateOptimalSchema().catch(console.error);
}

module.exports = { profileDataset, generateOptimalSchema };