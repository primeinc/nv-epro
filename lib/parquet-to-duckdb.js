#!/usr/bin/env node

/**
 * Parquet to DuckDB Cache Loader
 * 
 * Loads Parquet files into DuckDB for fast querying
 * DuckDB acts as a queryable cache of the canonical Parquet data
 */

const duckdb = require('@duckdb/node-api');
const path = require('path');
const fs = require('fs').promises;

let connection = null;
let instance = null;

/**
 * Get or create DuckDB connection
 */
async function getConnection(dbPath = ':memory:') {
  if (!connection) {
    instance = await duckdb.DuckDBInstance.create(dbPath);
    connection = await instance.connect();
  }
  return connection;
}

/**
 * Load Parquet files into DuckDB table
 */
async function loadParquetToTable(parquetPath, tableName, options = {}) {
  const conn = await getConnection(options.dbPath);
  
  try {
    // Determine if path is directory or file
    const stats = await fs.stat(parquetPath);
    const isDirectory = stats.isDirectory();
    
    const parquetGlob = isDirectory 
      ? `${parquetPath}/**/*.parquet`
      : parquetPath;
    
    // Create or replace table from Parquet files
    const sql = `
      CREATE OR REPLACE TABLE ${tableName} AS 
      SELECT * 
      FROM read_parquet('${parquetGlob.replace(/\\/g, '/')}')
    `;
    
    console.log(`Loading Parquet into DuckDB table '${tableName}'...`);
    await conn.run(sql);
    
    // Get statistics
    const statsReader = await conn.runAndReadAll(`
      SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT year) as year_count,
        COUNT(DISTINCT month) as month_count
      FROM ${tableName}
      WHERE year IS NOT NULL
    `);
    const statsWithDate = statsReader.getRows()[0];
    
    // If no year column, just get row count
    const simpleStatsReader = await conn.runAndReadAll(`
      SELECT COUNT(*) as row_count FROM ${tableName}
    `);
    const simpleStats = simpleStatsReader.getRows()[0];
    
    const rowCount = simpleStats[0];
    const yearCount = statsWithDate[1] || 0;
    const monthCount = statsWithDate[2] || 0;
    
    console.log(`✓ Loaded ${rowCount} rows into '${tableName}'`);
    if (yearCount > 0) {
      console.log(`  Covering ${yearCount} years, ${monthCount} months`);
    }
    
    return {
      success: true,
      tableName,
      rowCount,
      yearCount,
      monthCount
    };
    
  } catch (error) {
    console.error(`Failed to load Parquet into DuckDB:`, error);
    throw error;
  }
}

/**
 * Query DuckDB cache
 */
async function query(sql, options = {}) {
  const conn = await getConnection(options.dbPath);
  
  const reader = await conn.runAndReadAll(sql);
  return reader.getRows();
}

/**
 * Get table schema
 */
async function describeTable(tableName, options = {}) {
  const conn = await getConnection(options.dbPath);
  
  const reader = await conn.runAndReadAll(`DESCRIBE ${tableName}`);
  const rows = reader.getRows();
  
  return rows.map(row => ({
    column_name: row[0],
    column_type: row[1],
    nullable: row[2],
    key: row[3],
    default: row[4],
    extra: row[5]
  }));
}

/**
 * Close connection
 */
function closeConnection() {
  if (connection) {
    connection.disconnectSync();
    connection = null;
  }
  if (instance) {
    instance = null;
  }
}

module.exports = {
  loadParquetToTable,
  query,
  describeTable,
  closeConnection,
  getConnection
};