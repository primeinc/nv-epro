#!/usr/bin/env node

/**
 * Get EXACT optimal data types for all columns
 * No summaries, just precise type definitions
 */

const duckdb = require('@duckdb/node-api');
const fs = require('fs').promises;

async function getExactType(conn, table, col) {
  // Get column's actual DuckDB type
  const typeResult = await conn.runAndReadAll(`
    SELECT typeof("${col}") as dtype FROM ${table} LIMIT 1
  `);
  const duckdbType = typeResult.getRows()[0][0];
  
  // Get basic stats
  const stats = await conn.runAndReadAll(`
    SELECT 
      COUNT(*) as total,
      COUNT("${col}") as non_null,
      COUNT(DISTINCT "${col}") as unique_vals
    FROM ${table}
  `);
  const s = stats.getRows()[0];
  const total = Number(s[0]);
  const nonNull = Number(s[1]);
  const unique = Number(s[2]);
  const hasNulls = nonNull < total;
  
  let exactType;
  
  // Handle each column based on name and content
  if (duckdbType.includes('VARCHAR')) {
    // String type - get exact max length
    const lengthResult = await conn.runAndReadAll(`
      SELECT 
        MAX(LENGTH("${col}")) as max_len,
        MIN(LENGTH("${col}")) as min_len
      FROM ${table}
      WHERE "${col}" IS NOT NULL AND "${col}" != ''
    `);
    const maxLen = Number(lengthResult.getRows()[0][0]) || 0;
    const minLen = Number(lengthResult.getRows()[0][1]) || 0;
    
    // Check if column is entirely null/empty
    if (maxLen === 0 || nonNull === 0) {
      // Column has no data - make it nullable VARCHAR with reasonable default size
      exactType = col.includes('Address') ? 'VARCHAR(100)' :
                  col.includes('City') ? 'VARCHAR(50)' :
                  col.includes('State') ? 'VARCHAR(2)' :
                  col.includes('Postal') || col.includes('Zip') ? 'VARCHAR(10)' :
                  'VARCHAR(50)';
      return {
        column: col,
        exact_type: exactType,
        nullable: true,
        unique_values: unique,
        total_rows: total,
        cardinality: '0.00%'
      };
    }
    
    // Check patterns for specific types
    if (col.endsWith(' #') || col.endsWith('ID') || col === 'PO #' || col === 'Contract #' || col === 'Bid Solicitation #' || col === 'Vendor ID') {
      // ID columns need exact length + buffer
      exactType = `VARCHAR(${Math.min(maxLen + 10, 64)})`;
      if (unique === total) exactType += ' PRIMARY KEY';
      else if (!hasNulls) exactType += ' NOT NULL';
      
    } else if (col === 'Status') {
      // Get all distinct values for enum
      const vals = await conn.runAndReadAll(`
        SELECT DISTINCT "${col}" as val FROM ${table} 
        WHERE "${col}" IS NOT NULL AND "${col}" != ''
        ORDER BY val
      `);
      const values = vals.getRows().map(r => `'${r[0]}'`).join(', ');
      exactType = `VARCHAR(${maxLen + 5})`;
      if (unique <= 20 && values.length > 0) {
        exactType += ` CHECK ("${col}" IN (${values}))`;
      }
      if (!hasNulls) exactType += ' NOT NULL';
      
    } else if (col === 'Total' || col.includes('Dollar') || col.includes('Amount') || col.includes('Cost') || col === 'Dollars Spent to Date') {
      // Money column - check BEFORE date columns since "Dollars Spent to Date" includes "Date"
      const moneyCheck = await conn.runAndReadAll(`
        WITH parsed AS (
          SELECT 
            "${col}" as raw,
            TRY_CAST(REPLACE(REPLACE("${col}", '$', ''), ',', '') AS DECIMAL(38,4)) as val
          FROM ${table}
        )
        SELECT 
          MAX(ABS(val)) as max_abs,
          COUNT(CASE WHEN val != ROUND(val, 2) THEN 1 END) as needs_more_precision
        FROM parsed
      `);
      const m = moneyCheck.getRows()[0];
      const maxAbs = Number(m[0]);
      const needsPrecision = Number(m[1]) > 0;
      
      // Determine exact decimal precision needed
      if (maxAbs < 10000) {
        exactType = needsPrecision ? 'DECIMAL(8,4)' : 'DECIMAL(8,2)';
      } else if (maxAbs < 1000000) {
        exactType = needsPrecision ? 'DECIMAL(10,4)' : 'DECIMAL(10,2)';
      } else if (maxAbs < 100000000) {
        exactType = needsPrecision ? 'DECIMAL(12,4)' : 'DECIMAL(12,2)';
      } else if (maxAbs < 10000000000) {
        exactType = needsPrecision ? 'DECIMAL(15,4)' : 'DECIMAL(15,2)';
      } else {
        exactType = 'DECIMAL(18,2)';
      }
      if (!hasNulls) exactType += ' NOT NULL';
      
    } else if ((col.includes('Date') && !col.includes('Dollar')) || col === 'Bid Opening Date') {
      // Date column - check if parseable (but not Dollar columns with "Date" in name)
      // First check if column contains time component
      const hasTimeCheck = await conn.runAndReadAll(`
        SELECT COUNT(*) as has_time
        FROM ${table}
        WHERE "${col}" LIKE '%:%' AND "${col}" IS NOT NULL
        LIMIT 1
      `);
      const hasTime = Number(hasTimeCheck.getRows()[0][0]) > 0;
      
      // Use appropriate format based on content
      let dateCheck;
      if (hasTime) {
        // Has time component - parse as timestamp
        dateCheck = await conn.runAndReadAll(`
          SELECT 
            COUNT(*) as total,
            COUNT(TRY_CAST(strptime("${col}", '%m/%d/%Y %H:%M:%S') AS TIMESTAMP)) as valid
          FROM ${table}
          WHERE "${col}" IS NOT NULL AND "${col}" != ''
        `);
      } else {
        // No time component - parse as date
        dateCheck = await conn.runAndReadAll(`
          SELECT 
            COUNT(*) as total,
            COUNT(TRY_CAST(strptime("${col}", '%m/%d/%Y') AS DATE)) as valid
          FROM ${table}
          WHERE "${col}" IS NOT NULL AND "${col}" != ''
        `);
      }
      const d = dateCheck.getRows()[0];
      if (Number(d[1]) === Number(d[0]) && Number(d[0]) > 0) {
        // Successfully parsed all values
        if (hasTime) {
          exactType = hasNulls ? 'TIMESTAMP' : 'TIMESTAMP NOT NULL';
        } else {
          exactType = hasNulls ? 'DATE' : 'DATE NOT NULL';
        }
      } else {
        // Some values couldn't be parsed - keep as VARCHAR
        exactType = `VARCHAR(${maxLen})`;
        if (!hasNulls) exactType += ' NOT NULL';
      }
      
    } else {
      // Regular text columns
      if (unique < 50 && total > 100) {
        // Low cardinality - possible dimension
        exactType = `VARCHAR(${maxLen + 10})`;
        if (unique < 10) {
          const vals = await conn.runAndReadAll(`
            SELECT DISTINCT "${col}" as val FROM ${table} 
            WHERE "${col}" IS NOT NULL AND "${col}" != ''
            ORDER BY val
          `);
          const validRows = vals.getRows().filter(r => r[0] !== null && r[0] !== '');
          if (validRows.length > 0 && validRows.length < 10) {
            const values = validRows.map(r => `'${r[0]}'`).join(', ');
            if (values.length < 500) { // Don't make huge CHECK constraints
              exactType += ` CHECK ("${col}" IN (${values}))`;
            }
          }
        }
      } else {
        // Size based on actual max
        if (maxLen <= 32) exactType = `VARCHAR(${maxLen + 8})`;
        else if (maxLen <= 64) exactType = `VARCHAR(${maxLen + 16})`;
        else if (maxLen <= 128) exactType = `VARCHAR(${maxLen + 32})`;
        else if (maxLen <= 255) exactType = `VARCHAR(255)`;
        else exactType = 'TEXT';
      }
      if (!hasNulls) exactType += ' NOT NULL';
    }
    
  } else if (duckdbType === 'DATE') {
    exactType = hasNulls ? 'DATE' : 'DATE NOT NULL';
    
  } else if (duckdbType.includes('DECIMAL') || duckdbType.includes('DOUBLE')) {
    // Already numeric
    exactType = duckdbType;
    if (!hasNulls) exactType += ' NOT NULL';
    
  } else if (duckdbType.includes('INT')) {
    // Integer type
    const range = await conn.runAndReadAll(`
      SELECT MIN("${col}") as min_val, MAX("${col}") as max_val 
      FROM ${table}
    `);
    const r = range.getRows()[0];
    const minVal = Number(r[0]);
    const maxVal = Number(r[1]);
    
    if (minVal >= 0 && maxVal < 256) exactType = 'UTINYINT';
    else if (minVal >= -128 && maxVal < 128) exactType = 'TINYINT';
    else if (minVal >= 0 && maxVal < 65536) exactType = 'USMALLINT';
    else if (minVal >= -32768 && maxVal < 32768) exactType = 'SMALLINT';
    else if (minVal >= 0 && maxVal < 4294967296) exactType = 'UINTEGER';
    else if (minVal >= -2147483648 && maxVal < 2147483648) exactType = 'INTEGER';
    else exactType = 'BIGINT';
    
    if (!hasNulls) exactType += ' NOT NULL';
    
  } else {
    // Unknown type - keep as is
    exactType = duckdbType;
  }
  
  return {
    column: col,
    exact_type: exactType,
    nullable: hasNulls,
    unique_values: unique,
    total_rows: total,
    cardinality: (unique / total * 100).toFixed(2) + '%'
  };
}

async function profileDataset(dataset) {
  const conn = await duckdb.DuckDBConnection.create();
  
  console.log(`\n${'='.repeat(80)}`);
  console.log(`${dataset.toUpperCase()}`);
  console.log(`${'='.repeat(80)}`);
  
  const bronzePath = `data/bronze/${dataset}/*/*/data.parquet`;
  
  try {
    // Get columns
    const schema = await conn.runAndReadAll(`
      SELECT column_name 
      FROM (DESCRIBE SELECT * FROM read_parquet('${bronzePath}') LIMIT 1)
      WHERE column_name NOT IN (
        'source_system', 'source_file', 'source_file_hash', 'source_file_bytes',
        'source_row', 'ingested_at', 'bronze_run_id', 'row_hash',
        'ingest_date', 'sha256'
      )
      AND column_name NOT LIKE 'row_hash_%'
    `);
    
    const columns = schema.getRows().map(r => r[0]);
    const types = [];
    
    for (const col of columns) {
      try {
        const typeInfo = await getExactType(conn, `read_parquet('${bronzePath}')`, col);
        types.push(typeInfo);
        console.log(`${col}: ${typeInfo.exact_type}`);
      } catch (colErr) {
        console.log(`Error processing ${col}: ${colErr.message}`);
        types.push({
          column: col,
          exact_type: 'VARCHAR(255)',
          nullable: true,
          unique_values: 0,
          total_rows: 0,
          cardinality: '0.00%'
        });
      }
    }
    
    conn.disconnectSync();
    return types;
    
  } catch (e) {
    console.log(`Error: ${e.message}`);
    conn.disconnectSync();
    return null;
  }
}

async function main() {
  const datasets = ['purchase_orders', 'contracts', 'vendors', 'bids'];
  const allTypes = {};
  
  for (const dataset of datasets) {
    const types = await profileDataset(dataset);
    if (types) {
      allTypes[dataset] = types;
    }
  }
  
  // Save exact types
  const outputDir = 'config/bronze/validated';
  await fs.mkdir(outputDir, { recursive: true });
  const outputPath = `${outputDir}/bronze-schema-profile.json`;
  await fs.writeFile(outputPath, JSON.stringify(allTypes, null, 2));
  console.log(`\n\nExact types saved to ${outputPath}`);
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { profileDataset };