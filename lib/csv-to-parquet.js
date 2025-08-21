#!/usr/bin/env node

/**
 * CSV to Parquet Converter
 * 
 * Converts Nevada ePro CSV files to optimized Parquet format with proper types
 * This is the canonical storage format - Parquet files are the source of truth
 */

const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs').promises;
const { generateSelectSQL, getPartitionColumn } = require('./nevada-epro-schemas');

/**
 * Convert CSV to Parquet with optimized schema
 */
async function csvToParquet(csvPath, parquetPath, dataset, options = {}) {
  // Ensure output directory exists
  await fs.mkdir(parquetPath, { recursive: true });
  
  const conn = await duckdb.DuckDBConnection.create();
  
  try {
    // Build the query with schema transformation
    const schemaSQL = generateSelectSQL(dataset);
    
    // Add metadata columns
    const metadataColumns = options.runId ? `, '${options.runId}' AS run_id` : '';
    const timestampColumn = `, CURRENT_TIMESTAMP AS converted_at`;
    
    // Get partition column if any
    const partitionColumn = getPartitionColumn(dataset);
    
    let sql;
    if (partitionColumn && options.partitionByDate !== false) {
      // Partition by year/month of the date column
      sql = `
        COPY (
          ${schemaSQL}${metadataColumns}${timestampColumn},
          strftime(${partitionColumn}, '%Y') AS year,
          strftime(${partitionColumn}, '%m') AS month
          FROM read_csv_auto('${csvPath.replace(/\\/g, '/')}', 
            header=true, 
            ignore_errors=false,
            sample_size=-1
          )
        ) TO '${parquetPath.replace(/\\/g, '/')}'
        WITH (
          FORMAT PARQUET, 
          PARTITION_BY (year, month), 
          COMPRESSION ZSTD, 
          ROW_GROUP_SIZE 100000, 
          OVERWRITE_OR_IGNORE true
        )
      `;
    } else {
      // Non-partitioned output
      const outputFile = parquetPath.endsWith('.parquet') 
        ? parquetPath 
        : path.join(parquetPath, `${dataset}.parquet`);
        
      sql = `
        COPY (
          ${schemaSQL}${metadataColumns}${timestampColumn}
          FROM read_csv_auto('${csvPath.replace(/\\/g, '/')}', 
            header=true, 
            ignore_errors=false,
            sample_size=-1
          )
        ) TO '${outputFile.replace(/\\/g, '/')}'
        WITH (
          FORMAT PARQUET, 
          COMPRESSION ZSTD, 
          ROW_GROUP_SIZE 100000, 
          OVERWRITE_OR_IGNORE true
        )
      `;
    }
    
    console.log(`Converting ${dataset} CSV to Parquet...`);
    await conn.run(sql);
    
    // Get statistics
    const statsReader = await conn.runAndReadAll(`
      SELECT COUNT(*) as row_count 
      FROM read_parquet('${parquetPath.replace(/\\/g, '/')}/**/*.parquet')
    `);
    const stats = statsReader.getRows();
    
    console.log(`✓ Converted ${stats[0][0]} rows to Parquet`);
    
    return {
      success: true,
      rowCount: stats[0][0],
      outputPath: parquetPath
    };
    
  } catch (error) {
    console.error(`Failed to convert CSV to Parquet:`, error);
    throw error;
  } finally {
    conn.disconnectSync();
  }
}

module.exports = { csvToParquet };