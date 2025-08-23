#!/usr/bin/env node

/**
 * Profile Bronze data to determine optimal data types
 * Analyzes actual data patterns to define lossless transformations
 */

const duckdb = require('@duckdb/node-api');

async function profilePurchaseOrders() {
  const conn = await duckdb.DuckDBConnection.create();
  
  console.log('=== PURCHASE ORDERS DATA TYPE PROFILING ===\n');
  
  // Profile each column
  const columns = [
    'PO #', 'Description', 'Vendor', 'Organization', 
    'Department', 'Buyer', 'Status', 'Sent Date', 'Total'
  ];
  
  for (const col of columns) {
    console.log(`\n[${col}]`);
    
    // Get basic stats
    const stats = await conn.runAndReadAll(`
      SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT "${col}") as unique_values,
        COUNT(CASE WHEN "${col}" IS NULL THEN 1 END) as null_count,
        COUNT(CASE WHEN "${col}" = '' THEN 1 END) as empty_count,
        MIN(LENGTH("${col}")) as min_length,
        MAX(LENGTH("${col}")) as max_length,
        MODE("${col}") as most_common_value
      FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
    `);
    
    const s = stats.getRows()[0];
    console.log(`  Total: ${s[0]}, Unique: ${s[1]}, Nulls: ${s[2]}, Empty: ${s[3]}`);
    console.log(`  Length: ${s[4]}-${s[5]} chars`);
    console.log(`  Most common: "${s[6]}"`);
    
    // Column-specific analysis
    if (col === 'PO #') {
      // Check PO # patterns
      const patterns = await conn.runAndReadAll(`
        SELECT 
          COUNT(CASE WHEN "${col}" ~ '^[0-9]+$' THEN 1 END) as numeric_only,
          COUNT(CASE WHEN "${col}" ~ '^99' THEN 1 END) as starts_99,
          COUNT(CASE WHEN "${col}" LIKE '%:%' THEN 1 END) as has_colon,
          COUNT(CASE WHEN "${col}" LIKE '%.%' THEN 1 END) as has_dot,
          COUNT(CASE WHEN "${col}" LIKE '%-%' THEN 1 END) as has_dash
        FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
      `);
      const p = patterns.getRows()[0];
      console.log(`  Patterns: ${p[0]} numeric, ${p[1]} start with 99, ${p[2]} have colon`);
      console.log(`  Separators: ${p[4]} have dash, ${p[3]} have dot`);
      
      // Optimal type: VARCHAR(32) - handles all patterns, max seen is ~20 chars
      console.log(`  RECOMMENDED TYPE: VARCHAR(32) NOT NULL`);
    }
    
    else if (col === 'Total') {
      // Analyze amount patterns
      const amounts = await conn.runAndReadAll(`
        WITH parsed AS (
          SELECT 
            TRY_CAST(REPLACE(REPLACE("${col}", '$', ''), ',', '') AS DECIMAL(38,2)) as amount
          FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
        )
        SELECT 
          MIN(amount) as min_val,
          MAX(amount) as max_val,
          COUNT(CASE WHEN amount = 0 THEN 1 END) as zero_count,
          COUNT(CASE WHEN amount < 0 THEN 1 END) as negative_count,
          COUNT(CASE WHEN amount > 1000000 THEN 1 END) as over_million,
          COUNT(CASE WHEN amount > 10000000 THEN 1 END) as over_10mil
        FROM parsed
      `);
      const a = amounts.getRows()[0];
      console.log(`  Range: $${a[0]} to $${a[1]}`);
      console.log(`  Distribution: ${a[2]} zeros, ${a[3]} negative, ${a[4]} >$1M, ${a[5]} >$10M`);
      
      // Check precision needs
      const precision = await conn.runAndReadAll(`
        SELECT 
          COUNT(CASE WHEN "${col}" LIKE '%.%' AND "${col}" NOT LIKE '%.00' THEN 1 END) as has_cents,
          MAX(LENGTH(REGEXP_REPLACE("${col}", '[^0-9]', ''))) as max_digits
        FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
      `);
      const pr = precision.getRows()[0];
      console.log(`  Precision: ${pr[0]} have cents, max ${pr[1]} digits`);
      
      // Optimal type: DECIMAL(12,2) - handles up to $9,999,999,999.99
      console.log(`  RECOMMENDED TYPE: DECIMAL(12,2) NOT NULL DEFAULT 0`);
    }
    
    else if (col === 'Sent Date') {
      // Analyze date patterns
      const dates = await conn.runAndReadAll(`
        WITH parsed AS (
          SELECT 
            "${col}" as raw_date,
            TRY_CAST(strptime("${col}", '%m/%d/%Y') AS DATE) as parsed_date
          FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
        )
        SELECT 
          COUNT(*) as total,
          COUNT(parsed_date) as parseable,
          MIN(parsed_date) as earliest,
          MAX(parsed_date) as latest,
          COUNT(CASE WHEN parsed_date > CURRENT_DATE THEN 1 END) as future_dates
        FROM parsed
      `);
      const d = dates.getRows()[0];
      console.log(`  Parseable: ${d[1]}/${d[0]}`);
      console.log(`  Range: ${d[2]} to ${d[3]}`);
      console.log(`  Future dates: ${d[4]}`);
      
      // Optimal type: DATE NOT NULL
      console.log(`  RECOMMENDED TYPE: DATE NOT NULL`);
    }
    
    else if (col === 'Status') {
      // Get all unique statuses
      const statuses = await conn.runAndReadAll(`
        SELECT "${col}", COUNT(*) as count
        FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
        GROUP BY "${col}"
        ORDER BY count DESC
        LIMIT 20
      `);
      console.log(`  Status values:`);
      statuses.getRows().forEach(r => console.log(`    "${r[0]}": ${r[1]}`));
      
      // Optimal type: ENUM or VARCHAR(32)
      console.log(`  RECOMMENDED TYPE: VARCHAR(32) NOT NULL`);
    }
    
    else {
      // Text fields - check if should be normalized
      const topValues = await conn.runAndReadAll(`
        SELECT "${col}", COUNT(*) as count
        FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
        GROUP BY "${col}"
        ORDER BY count DESC
        LIMIT 5
      `);
      console.log(`  Top values:`);
      topValues.getRows().forEach(r => console.log(`    "${r[0]}": ${r[1]}`));
      
      // Check if should be dimension table
      const cardinality = await conn.runAndReadAll(`
        SELECT 
          COUNT(DISTINCT "${col}") as unique_count,
          COUNT(*) as total_count,
          CAST(COUNT(DISTINCT "${col}") AS FLOAT) / COUNT(*) as cardinality_ratio
        FROM read_parquet('data/bronze/purchase_orders/*/*/data.parquet')
      `);
      const c = cardinality.getRows()[0];
      
      if (Number(c[2]) < 0.01) {
        console.log(`  Low cardinality (${(Number(c[2]) * 100).toFixed(2)}%) - CANDIDATE FOR DIMENSION TABLE`);
        console.log(`  RECOMMENDED TYPE: INTEGER REFERENCES dim_${col.toLowerCase().replace(' ', '_')}`);
      } else {
        console.log(`  High cardinality (${(Number(c[2]) * 100).toFixed(2)}%)`);
        const maxLen = Number(s[5]);
        console.log(`  RECOMMENDED TYPE: VARCHAR(${maxLen < 255 ? maxLen + 20 : 255})`);
      }
    }
  }
  
  conn.disconnectSync();
}

async function profileAllDatasets() {
  await profilePurchaseOrders();
  // TODO: Add other datasets
}

if (require.main === module) {
  profileAllDatasets().catch(console.error);
}

module.exports = { profileAllDatasets };