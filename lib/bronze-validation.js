#!/usr/bin/env node

/**
 * Bronze Layer Validation - Production Grade
 * 
 * Outputs structured JSON following data observability best practices
 * Compatible with monitoring systems (DataDog, Splunk, etc.)
 * Exit codes: 0=healthy, 1=warning, 2=critical
 */

const duckdb = require('@duckdb/node-api');
const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');

/**
 * Calculate statistical distribution metrics
 */
function calculateDistribution(values) {
  if (!values || values.length === 0) return null;
  
  const sorted = values.slice().sort((a, b) => a - b);
  const n = sorted.length;
  
  return {
    min: sorted[0],
    p25: sorted[Math.floor(n * 0.25)],
    p50: sorted[Math.floor(n * 0.50)],
    p75: sorted[Math.floor(n * 0.75)],
    p95: sorted[Math.floor(n * 0.95)],
    p99: sorted[Math.floor(n * 0.99)],
    max: sorted[n - 1],
    mean: values.reduce((a, b) => a + b, 0) / n,
    stddev: Math.sqrt(values.reduce((sq, n, i, arr) => {
      const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
      return sq + Math.pow(n - mean, 2);
    }, 0) / n)
  };
}

/**
 * Validate Bronze dataset and return observability metrics
 */
async function validateBronzeDataset(conn, datasetName, bronzePath) {
  const metrics = {
    dataset: datasetName,
    path: bronzePath,
    timestamp: new Date().toISOString(),
    status: 'unknown',
    metrics: {},
    anomalies: [],
    schema: {},
    lineage: {},
    distribution: {}
  };
  
  try {
    // Extract metadata from path
    const hashMatch = bronzePath.match(/sha256=([a-f0-9]{64})/);
    const dateMatch = bronzePath.match(/ingest_date=(\d{4}-\d{2}-\d{2})/);
    
    metrics.lineage = {
      content_hash: hashMatch ? hashMatch[1] : null,
      ingest_date: dateMatch ? dateMatch[1] : null,
      path_valid: !!(hashMatch && dateMatch)
    };
    
    // VOLUME metrics
    const volumeSql = `
      SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT row_hash) as unique_rows,
        MIN(LENGTH(row_hash)) as min_hash_length,
        MAX(LENGTH(row_hash)) as max_hash_length,
        COUNT(DISTINCT source_file_hash) as source_files,
        MIN(source_file_bytes) as min_bytes,
        MAX(source_file_bytes) as max_bytes
      FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
    `;
    
    const volumeReader = await conn.runAndReadAll(volumeSql);
    const volume = volumeReader.getRows()[0];
    
    metrics.metrics.volume = {
      row_count: Number(volume[0]),
      unique_rows: Number(volume[1]),
      duplicate_rows: Number(volume[0]) - Number(volume[1]),
      duplication_rate: (Number(volume[0]) - Number(volume[1])) / Number(volume[0]),
      source_files: Number(volume[4]),
      bytes_min: Number(volume[5]),
      bytes_max: Number(volume[6])
    };
    
    // FRESHNESS metrics
    const freshnessSql = `
      SELECT 
        MIN(ingested_at) as first_ingested,
        MAX(ingested_at) as last_ingested,
        COUNT(DISTINCT DATE(ingested_at)) as ingestion_days,
        MAX(bronze_run_id) as latest_run_id
      FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
    `;
    
    const freshnessReader = await conn.runAndReadAll(freshnessSql);
    const freshness = freshnessReader.getRows()[0];
    
    const lastIngested = new Date(freshness[1]);
    const ageHours = (Date.now() - lastIngested) / (1000 * 60 * 60);
    
    metrics.metrics.freshness = {
      first_ingested: freshness[0],
      last_ingested: freshness[1],
      age_hours: ageHours,
      ingestion_days: Number(freshness[2]),
      latest_run_id: freshness[3],
      is_stale: ageHours > 24
    };
    
    // SCHEMA metrics
    const schemaSql = `
      SELECT 
        column_name,
        column_type,
        null_percentage
      FROM (
        SUMMARIZE SELECT * FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
      )
      WHERE column_name NOT LIKE '%ingest%'
        AND column_name NOT LIKE '%source%'
        AND column_name NOT LIKE '%bronze%'
        AND column_name NOT LIKE '%row_hash%'
    `;
    
    const schemaReader = await conn.runAndReadAll(schemaSql);
    const schemaRows = schemaReader.getRows();
    
    metrics.schema = {
      column_count: schemaRows.length,
      columns: schemaRows.map(row => ({
        name: row[0],
        type: row[1],
        null_rate: parseFloat(row[2]) || 0
      }))
    };
    
    // DISTRIBUTION metrics (sample key columns)
    const distColumns = {
      bids: 'Bid Opening Date',
      contracts: 'Start Date',
      purchase_orders: 'Sent Date',
      vendors: 'State'
    };
    
    const keyColumn = distColumns[datasetName];
    if (keyColumn) {
      // Check for nulls and anomalies
      const nullCheckSql = `
        SELECT 
          COUNT(*) FILTER (WHERE "${keyColumn}" IS NULL) as nulls,
          COUNT(*) FILTER (WHERE "${keyColumn}" = '') as empty,
          COUNT(DISTINCT "${keyColumn}") as distinct_values,
          MIN("${keyColumn}") as min_value,
          MAX("${keyColumn}") as max_value
        FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
      `;
      
      const nullReader = await conn.runAndReadAll(nullCheckSql);
      const nullStats = nullReader.getRows()[0];
      
      metrics.distribution[keyColumn] = {
        null_count: Number(nullStats[0]),
        empty_count: Number(nullStats[1]),
        distinct_values: Number(nullStats[2]),
        min_value: nullStats[3],
        max_value: nullStats[4],
        null_rate: Number(nullStats[0]) / metrics.metrics.volume.row_count,
        cardinality: Number(nullStats[2]) / metrics.metrics.volume.row_count
      };
      
      // Flag anomalies
      const nullRate = metrics.distribution[keyColumn].null_rate;
      if (nullRate > 0.5) {
        metrics.anomalies.push({
          severity: 'warning',
          type: 'high_null_rate',
          column: keyColumn,
          value: nullRate,
          threshold: 0.5,
          message: `${keyColumn} has ${(nullRate * 100).toFixed(1)}% NULL values`
        });
      }
    }
    
    // Dataset-specific anomaly detection
    if (datasetName === 'purchase_orders' && metrics.metrics.volume.row_count < 100) {
      metrics.anomalies.push({
        severity: 'warning',
        type: 'low_volume',
        dataset: datasetName,
        value: metrics.metrics.volume.row_count,
        expected_min: 100,
        message: `Only ${metrics.metrics.volume.row_count} purchase orders (expected 100+)`
      });
    }
    
    if (datasetName === 'vendors' && metrics.schema.columns.find(c => c.name === 'State' && c.null_rate > 0.9)) {
      metrics.anomalies.push({
        severity: 'info',
        type: 'data_quality',
        dataset: datasetName,
        column: 'State',
        message: 'Vendor state data largely missing - possible extraction issue'
      });
    }
    
    // Determine overall status
    if (metrics.anomalies.some(a => a.severity === 'critical')) {
      metrics.status = 'critical';
    } else if (metrics.anomalies.some(a => a.severity === 'warning')) {
      metrics.status = 'warning';
    } else {
      metrics.status = 'healthy';
    }
    
    // Add SLI metrics (Service Level Indicators)
    metrics.sli = {
      completeness: metrics.metrics.volume.unique_rows / metrics.metrics.volume.row_count,
      uniqueness: 1 - metrics.metrics.volume.duplication_rate,
      freshness: ageHours < 24 ? 1 : 0,
      validity: metrics.lineage.path_valid ? 1 : 0
    };
    
    return metrics;
    
  } catch (error) {
    metrics.status = 'error';
    metrics.error = {
      message: error.message,
      stack: error.stack
    };
    return metrics;
  }
}

/**
 * Main validation orchestrator
 */
async function validateBronze(options = {}) {
  const startTime = Date.now();
  const bronzeBasePath = options.bronzeBasePath || 'data/bronze';
  const outputFormat = options.format || 'json';
  const conn = await duckdb.DuckDBConnection.create();
  
  const report = {
    run_id: `bronze_validate_${Date.now()}`,
    timestamp: new Date().toISOString(),
    environment: {
      bronze_base_path: bronzeBasePath,
      hostname: require('os').hostname(),
      node_version: process.version,
      duckdb_version: '0.10.0'  // Would query this dynamically
    },
    summary: {
      datasets_scanned: 0,
      healthy: 0,
      warning: 0,
      critical: 0,
      error: 0,
      total_rows: 0,
      total_bytes: 0
    },
    datasets: {},
    anomalies: [],
    slo: {  // Service Level Objectives
      target_completeness: 0.99,
      target_freshness_hours: 24,
      target_uniqueness: 1.0,
      target_availability: 0.999
    },
    performance: {}
  };
  
  try {
    // Find all Bronze datasets
    const fg = require('fast-glob');
    const pattern = path.join(bronzeBasePath, '**/data.parquet').replace(/\\/g, '/');
    const parquetFiles = await fg(pattern);
    
    // Group by dataset
    const byDataset = {};
    for (const file of parquetFiles) {
      const match = file.match(/bronze\/([^\/]+)\//);
      if (match) {
        const dataset = match[1];
        if (!byDataset[dataset]) byDataset[dataset] = [];
        byDataset[dataset].push(file);
      }
    }
    
    // Validate each dataset (use latest version)
    for (const [dataset, files] of Object.entries(byDataset)) {
      const latestFile = files.sort().pop();  // Get most recent
      const validation = await validateBronzeDataset(conn, dataset, latestFile);
      
      report.datasets[dataset] = validation;
      report.summary.datasets_scanned++;
      report.summary[validation.status]++;
      report.summary.total_rows += validation.metrics.volume?.row_count || 0;
      report.summary.total_bytes += validation.metrics.volume?.bytes_max || 0;
      
      // Collect all anomalies
      if (validation.anomalies && validation.anomalies.length > 0) {
        report.anomalies.push(...validation.anomalies.map(a => ({
          dataset,
          ...a
        })));
      }
    }
    
    // Calculate aggregate SLI
    const allSLIs = Object.values(report.datasets)
      .filter(d => d.sli)
      .map(d => d.sli);
    
    if (allSLIs.length > 0) {
      report.aggregate_sli = {
        completeness: allSLIs.reduce((sum, s) => sum + s.completeness, 0) / allSLIs.length,
        uniqueness: allSLIs.reduce((sum, s) => sum + s.uniqueness, 0) / allSLIs.length,
        freshness: allSLIs.reduce((sum, s) => sum + s.freshness, 0) / allSLIs.length,
        validity: allSLIs.reduce((sum, s) => sum + s.validity, 0) / allSLIs.length
      };
      
      // Check SLO compliance
      report.slo_compliance = {
        completeness: report.aggregate_sli.completeness >= report.slo.target_completeness,
        uniqueness: report.aggregate_sli.uniqueness >= report.slo.target_uniqueness,
        freshness: report.aggregate_sli.freshness >= (23/24),  // 23 hours fresh
        all_met: false
      };
      report.slo_compliance.all_met = Object.values(report.slo_compliance)
        .filter(v => typeof v === 'boolean')
        .every(v => v);
    }
    
    // Performance metrics
    report.performance = {
      validation_duration_ms: Date.now() - startTime,
      datasets_per_second: report.summary.datasets_scanned / ((Date.now() - startTime) / 1000),
      rows_per_second: report.summary.total_rows / ((Date.now() - startTime) / 1000)
    };
    
    // Determine exit code
    report.exit_code = 0;
    if (report.summary.critical > 0) report.exit_code = 2;
    else if (report.summary.warning > 0) report.exit_code = 1;
    
  } catch (error) {
    report.error = {
      message: error.message,
      stack: error.stack
    };
    report.exit_code = 3;
  } finally {
    conn.disconnectSync();
  }
  
  // Output based on format (handle BigInt serialization)
  const replacer = (key, value) => 
    typeof value === 'bigint' ? Number(value) : value;
  
  if (outputFormat === 'json') {
    console.log(JSON.stringify(report, replacer, 2));
  } else if (outputFormat === 'ndjson') {
    // Newline-delimited JSON for streaming
    Object.entries(report.datasets).forEach(([name, data]) => {
      console.log(JSON.stringify({ dataset: name, ...data }, replacer));
    });
  }
  
  return report;
}

// Export for use as library
module.exports = { validateBronze, validateBronzeDataset };

// CLI execution
if (require.main === module) {
  const args = process.argv.slice(2);
  const options = {
    format: args.includes('--ndjson') ? 'ndjson' : 'json',
    bronzeBasePath: args.find(a => a.startsWith('--path='))?.split('=')[1] || 'data/bronze'
  };
  
  validateBronze(options)
    .then(report => process.exit(report.exit_code || 0))
    .catch(err => {
      console.error(err);
      process.exit(3);
    });
}