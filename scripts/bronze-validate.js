#!/usr/bin/env node

/**
 * Bronze Layer Validation
 * 
 * Validates Bronze layer data quality and emits OpenLineage events
 * with Great Expectations validation results as facets.
 * 
 * Exit codes:
 * 0 - All checks pass
 * 1 - Warnings only (within tolerances)
 * 2 - Validation failure
 * 3 - IO/manifest corruption
 */

const path = require('path');
const fs = require('fs').promises;
const duckdb = require('@duckdb/node-api');
const kleur = require('kleur');
const { 
  createBronzeValidationEvent, 
  createExpectation,
  emitEvent,
  writeOTelLog,
  generateRunId
} = require('../lib/openlineage-emitter');

/**
 * Run validation expectations on Bronze data
 */
async function runExpectations(conn, dataset, bronzePath, config = {}) {
  const expectations = [];
  const startTime = Date.now();
  
  // Row count expectation
  const rowCountSql = `SELECT COUNT(*) as count FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')`;
  const rowCountReader = await conn.runAndReadAll(rowCountSql);
  const rowCount = Number(rowCountReader.getRows()[0][0]);
  
  expectations.push(createExpectation(
    'expect_table_row_count_to_be_between',
    { min_value: config.min_rows || 1, max_value: config.max_rows || 10000000 },
    { 
      success: rowCount >= (config.min_rows || 1) && rowCount <= (config.max_rows || 10000000),
      observed_value: rowCount 
    }
  ));
  
  // Column null checks
  const schema = require('../config/bronze/schema-registry.json').schemas[dataset]?.['v0.1.0'];
  if (schema?.columns) {
    for (const [column, type] of Object.entries(schema.columns)) {
      const nullSql = `
        SELECT 
          COUNT(*) as total,
          COUNT(CASE WHEN "${column}" IS NULL THEN 1 END) as null_count
        FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
      `;
      
      const nullReader = await conn.runAndReadAll(nullSql);
      const nullResult = nullReader.getRows()[0];
      const total = Number(nullResult[0]);
      const nullCount = Number(nullResult[1]);
      const nullPercent = nullCount / total;
      const maxNullRate = config.expectations?.null_rate_max?.[column] || 0.5;
      
      expectations.push(createExpectation(
        'expect_column_values_to_not_be_null',
        { column, mostly: 1 - maxNullRate },
        {
          success: nullPercent <= maxNullRate,
          unexpected_count: nullCount,
          unexpected_percent: nullPercent * 100,
          observed_value: total - nullCount
        }
      ));
    }
  }
  
  // Primary key uniqueness check
  const primaryKey = config.pk || config.primary_key;
  if (primaryKey && primaryKey.length > 0) {
    const pkColumns = primaryKey.map(c => `"${c}"`).join(', ');
    const uniqueSql = `
      SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT ${pkColumns}) as unique_count
      FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
    `;
    
    const uniqueReader = await conn.runAndReadAll(uniqueSql);
    const uniqueResult = uniqueReader.getRows()[0];
    const total = Number(uniqueResult[0]);
    const unique = Number(uniqueResult[1]);
    
    expectations.push(createExpectation(
      'expect_compound_columns_to_be_unique',
      { column_list: primaryKey },
      {
        success: total === unique,
        observed_value: unique,
        unexpected_count: total - unique
      }
    ));
  }
  
  // Row hash uniqueness check
  const hashSql = `
    SELECT 
      COUNT(*) as total,
      COUNT(DISTINCT row_hash) as unique_hashes
    FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
  `;
  
  const hashReader = await conn.runAndReadAll(hashSql);
  const hashResult = hashReader.getRows()[0];
  const totalRows = Number(hashResult[0]);
  const uniqueHashes = Number(hashResult[1]);
  
  expectations.push(createExpectation(
    'expect_table_row_hash_to_be_unique',
    { column: 'row_hash' },
    {
      success: totalRows === uniqueHashes,
      observed_value: uniqueHashes,
      unexpected_count: totalRows - uniqueHashes,
      unexpected_percent: ((totalRows - uniqueHashes) / totalRows) * 100
    }
  ));
  
  // Content hash consistency
  const contentHashSql = `
    SELECT COUNT(DISTINCT source_file_hash) as unique_source_hashes
    FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
  `;
  
  const contentReader = await conn.runAndReadAll(contentHashSql);
  const contentResult = contentReader.getRows()[0];
  const uniqueSourceHashes = Number(contentResult[0]);
  
  // Extract expected hash from path
  const hashMatch = bronzePath.match(/sha256=([a-f0-9]{64})/);
  const expectedHash = hashMatch ? hashMatch[1] : null;
  
  expectations.push(createExpectation(
    'expect_content_hash_consistency',
    { expected_unique_hashes: 1 },
    {
      success: uniqueSourceHashes === 1,
      observed_value: uniqueSourceHashes
    }
  ));
  
  const duration = Date.now() - startTime;
  
  return { expectations, duration, rowCount };
}

/**
 * Calculate validation metrics for Bronze data
 */
async function calculateMetrics(conn, bronzePath) {
  const metrics = {};
  
  // Volume metrics
  const volumeSql = `
    SELECT 
      COUNT(*) as row_count,
      COUNT(DISTINCT row_hash) as unique_rows,
      MIN(source_file_bytes) as min_bytes,
      MAX(source_file_bytes) as max_bytes,
      AVG(source_file_bytes) as avg_bytes
    FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
  `;
  
  const volumeReader = await conn.runAndReadAll(volumeSql);
  const volume = volumeReader.getRows()[0];
  
  metrics.volume = {
    row_count: Number(volume[0]),
    unique_rows: Number(volume[1]),
    duplicate_rows: Number(volume[0]) - Number(volume[1]),
    bytes_min: Number(volume[2]),
    bytes_max: Number(volume[3]),
    bytes_avg: Number(volume[4])
  };
  
  // Freshness metrics
  const freshnessSql = `
    SELECT 
      MIN(ingested_at) as first_ingested,
      MAX(ingested_at) as last_ingested,
      COUNT(DISTINCT DATE(ingested_at)) as ingestion_days
    FROM read_parquet('${bronzePath.replace(/\\/g, '/')}')
  `;
  
  const freshnessReader = await conn.runAndReadAll(freshnessSql);
  const freshness = freshnessReader.getRows()[0];
  
  const lastIngested = new Date(freshness[1]);
  const ageHours = (Date.now() - lastIngested) / (1000 * 60 * 60);
  
  metrics.freshness = {
    first_ingested: freshness[0],
    last_ingested: freshness[1],
    age_hours: ageHours,
    ingestion_days: Number(freshness[2])
  };
  
  return metrics;
}

/**
 * Validate Bronze layer data
 */
async function validateBronze(options) {
  const { dataset, bronzePath, inputs = [], config = {}, format = 'pretty' } = options;
  
  const eventTime = new Date().toISOString();
  const runId = generateRunId(eventTime, bronzePath);
  
  // Write OTel log for start
  await writeOTelLog('INFO', 'bronze.validate.start', {
    dataset,
    runId,
    bronzePath
  });
  
  if (format === 'pretty') {
    console.log(kleur.cyan(`Validating ${kleur.bold(dataset)}...`));
  }
  
  const conn = await duckdb.DuckDBConnection.create();
  
  try {
    // Run expectations
    const { expectations, duration, rowCount } = await runExpectations(conn, dataset, bronzePath, config);
    const metrics = await calculateMetrics(conn, bronzePath);
    
    // Determine validation status
    const failedExpectations = expectations.filter(e => !e.success);
    const criticalFailures = failedExpectations.filter(e => {
      const type = e.expectation_config.expectation_type;
      // Only row_count and content_hash are critical
      // Duplicates (unique) are warnings since this is raw data
      return type.includes('row_count') || 
             type.includes('content_hash');
    });
    
    // Intermediate results only in pretty mode
    if (format === 'pretty' && failedExpectations.length > 0) {
      const passCount = expectations.length - failedExpectations.length;
      const warnings = failedExpectations.filter(e => {
        const type = e.expectation_config.expectation_type;
        return type.includes('unique') || type.includes('hash');
      });
      
      if (warnings.length === failedExpectations.length) {
        console.log(`${kleur.yellow(`Found ${warnings.length} warning(s)`)} (${passCount}/${expectations.length} passed)`);
      } else {
        console.log(`${kleur.yellow(`Found ${failedExpectations.length} issues`)} (${passCount}/${expectations.length} passed)`);
      }
    }
    
    let status = 'healthy';
    let exitCode = 0;
    
    if (criticalFailures.length > 0) {
      status = 'failed';
      exitCode = 2;
    } else if (failedExpectations.length > 0) {
      status = 'warning';
      exitCode = 1;
    }
    
    console.log(`DEBUG: exitCode=${exitCode}, status=${status}, failed=${failedExpectations.length}, critical=${criticalFailures.length}`);
    
    // Create validation results
    const validationResults = {
      status,
      expectations,
      metrics,
      dataset,
      timestamp: eventTime
    };
    
    // Create and emit OpenLineage event
    const { event, validationResult } = createBronzeValidationEvent({
      dataset,
      bronzePath,
      validationResults,
      runId
    });
    
    await emitEvent(event);
    
    // Write Great Expectations result separately
    const geResultPath = path.join('logs', 'validation', `${dataset}_${runId}.json`);
    await fs.mkdir(path.dirname(geResultPath), { recursive: true });
    await fs.writeFile(geResultPath, JSON.stringify(validationResult, null, 2));
    
    // Update Bronze manifest with validation results
    const manifestPath = path.join(path.dirname(bronzePath), 'manifest.json');
    if (format === 'pretty') {
      console.log(kleur.dim(`Updating manifest: ${manifestPath}`));
    }
    try {
      // Read existing manifest
      const manifestContent = await fs.readFile(manifestPath, 'utf-8');
      const manifest = JSON.parse(manifestContent);
      
      // Helper to convert BigInt to Number and handle Date objects in nested structures
      const serializeMetrics = (obj) => {
        if (obj === null || obj === undefined) return obj;
        if (typeof obj === 'bigint') return Number(obj);
        if (obj instanceof Date) return obj.toISOString();
        // Handle DuckDB date/timestamp objects which may have micros property
        if (typeof obj === 'object' && 'micros' in obj && Object.keys(obj).length === 1) {
          return new Date(Number(obj.micros) / 1000).toISOString();
        }
        if (typeof obj !== 'object') return obj;
        if (Array.isArray(obj)) return obj.map(serializeMetrics);
        
        const result = {};
        for (const [key, value] of Object.entries(obj)) {
          result[key] = serializeMetrics(value);
        }
        return result;
      };
      
      // Add validation results to manifest
      manifest.validation = {
        ...manifest.validation,  // Keep existing basic validation
        status,
        timestamp: eventTime,
        run_id: runId,
        passed: criticalFailures.length === 0,
        has_warnings: failedExpectations.length > criticalFailures.length,
        statistics: {
          evaluated_expectations: validationResult.statistics.evaluated_expectations,
          successful_expectations: validationResult.statistics.successful_expectations,
          unsuccessful_expectations: validationResult.statistics.unsuccessful_expectations
        },
        failures: criticalFailures.map(e => ({
          type: e.expectation_config.expectation_type,
          kwargs: e.expectation_config.kwargs,
          observed_value: serializeMetrics(e.result.observed_value),
          unexpected_count: serializeMetrics(e.result.unexpected_count)
        })),
        warnings: failedExpectations.filter(e => !criticalFailures.includes(e)).map(e => ({
          type: e.expectation_config.expectation_type,
          kwargs: e.expectation_config.kwargs,
          observed_value: serializeMetrics(e.result.observed_value),
          unexpected_count: serializeMetrics(e.result.unexpected_count),
          unexpected_percent: serializeMetrics(e.result.unexpected_percent)
        })),
        metrics: serializeMetrics(metrics)
      };
      
      // Write updated manifest back
      await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
      
      if (format === 'pretty') {
        console.log(kleur.dim(`Manifest updated: ${manifestPath}`));
      }
    } catch (error) {
      // Log but don't fail if manifest update fails
      if (format === 'pretty') {
        console.log(kleur.red(`Failed to update manifest: ${error.message}`));
      }
      await writeOTelLog('WARN', 'bronze.validate.manifest_update_failed', {
        dataset,
        runId,
        error: error.message
      });
    }
    
    // Write OTel log for completion
    await writeOTelLog('INFO', 'bronze.validate.complete', {
      dataset,
      runId,
      rows: rowCount,
      success: validationResult.success,
      duration_ms: duration,
      evaluated_expectations: validationResult.statistics.evaluated_expectations,
      successful_expectations: validationResult.statistics.successful_expectations
    });
    
    // Output results based on format
    if (format === 'json') {
      // Clean structured output for CI consumption
      const jsonResult = {
        dataset,
        status,
        exit_code: exitCode,
        timestamp: eventTime,
        run_id: runId,
        row_count: rowCount,
        expectations_passed: expectations.length - failedExpectations.length,
        expectations_total: expectations.length,
        duration_ms: duration,
        failed_checks: failedExpectations.map(e => {
          const result = {
            type: e.expectation_config.expectation_type.replace('expect_', ''),
            success: false
          };
          
          if (e.expectation_config.kwargs?.column) {
            result.column = e.expectation_config.kwargs.column;
          }
          
          if (e.expectation_config.expectation_type === 'expect_column_values_to_not_be_null') {
            result.null_percent = e.result.unexpected_percent || 0;
            result.threshold_percent = (1 - e.expectation_config.kwargs.mostly) * 100;
          } else {
            result.observed = e.result.observed_value;
          }
          
          return result;
        })
      };
      console.log(JSON.stringify(jsonResult, null, 2));
    } else {
      // Pretty output with strategic colors
      const statusText = exitCode === 0 ? 'PASS' : exitCode === 1 ? 'WARN' : 'FAIL';
      const statusColor = exitCode === 0 ? kleur.green : exitCode === 1 ? kleur.yellow : kleur.red;
      
      console.log(`\n${statusColor().bold(`BRONZE VALIDATION: ${statusText}`)}`);
      console.log(`Dataset: ${kleur.cyan().bold(dataset)}`);
      console.log(`Rows: ${kleur.magenta().bold(rowCount.toLocaleString())}`);
      
      const passCount = expectations.length - failedExpectations.length;
      const passColor = passCount === expectations.length ? kleur.green : kleur.yellow;
      console.log(`Checks: ${passColor().bold(passCount)}${kleur.gray('/')}${kleur.bold(expectations.length)} passed`);
      
      if (failedExpectations.length > 0) {
        // Separate warnings from failures
        const warnings = failedExpectations.filter(e => {
          const type = e.expectation_config.expectation_type;
          return type.includes('unique') || type.includes('hash');
        });
        const failures = failedExpectations.filter(e => !warnings.includes(e));
        
        if (failures.length > 0) {
          console.log(`\n${kleur.red().bold('FAILURES:')}`);
          failures.forEach(e => {
            const config = e.expectation_config;
            const result = e.result;
            
            if (config.expectation_type === 'expect_column_values_to_not_be_null') {
              const column = config.kwargs.column;
              const actualNullPercent = result.unexpected_percent || 0;
              const threshold = (1 - config.kwargs.mostly) * 100;
              const percentColor = actualNullPercent > threshold * 1.5 ? kleur.red : kleur.yellow;
              console.log(`  ${kleur.cyan(column)}: ${percentColor().bold(actualNullPercent.toFixed(1) + '%')} null (max: ${kleur.gray(threshold.toFixed(1) + '%')})`);
            } else if (config.expectation_type === 'expect_table_row_count_to_be_between') {
              const actual = result.observed_value;
              const min = config.kwargs.min_value;
              const max = config.kwargs.max_value;
              console.log(`  Row count: ${kleur.red().bold(actual)} (expected: ${kleur.gray(min + '-' + max)})`);
            } else {
              console.log(`  ${config.expectation_type}: ${kleur.red().bold(JSON.stringify(result.observed_value))}`);
            }
          });
        }
        
        if (warnings.length > 0) {
          console.log(`\n${kleur.yellow().bold('WARNINGS:')}`);
          warnings.forEach(e => {
            const config = e.expectation_config;
            const result = e.result;
            
            if (config.expectation_type.includes('unique')) {
              const actual = result.observed_value;
              const unexpected = result.unexpected_count || 0;
              console.log(`  Duplicates: ${kleur.yellow().bold(unexpected)} (unique: ${kleur.gray(actual)})`);
            } else {
              console.log(`  ${config.expectation_type}: ${kleur.yellow().bold(JSON.stringify(result.observed_value))}`);
            }
          });
        }
      }
      
      console.log(`\n${kleur.dim(`Duration: ${duration}ms | Logs: logs/validation/${dataset}_${runId}.json`)}`);
    }
    
    conn.disconnectSync();
    
    return {
      exitCode,
      event,
      validationResult
    };
    
  } catch (error) {
    // Write OTel error log
    await writeOTelLog('ERROR', 'bronze.validate.failed', {
      dataset,
      runId,
      error: error.message
    });
    
    conn.disconnectSync();
    
    // IO/corruption error
    return {
      exitCode: 3,
      error: error.message
    };
  }
}

/**
 * Validate all Bronze datasets
 */
async function validateAllBronze(options = {}) {
  const bronzeBasePath = options.bronzeBasePath || 'data/bronze';
  const format = options.format || 'pretty';
  const fg = require('fast-glob');
  
  // Find all Bronze parquet files
  const pattern = path.join(bronzeBasePath, '**/data.parquet').replace(/\\/g, '/');
  const parquetFiles = await fg(pattern);
  
  // Group by dataset and get latest
  const byDataset = {};
  for (const file of parquetFiles) {
    const match = file.match(/bronze[\/\\]([^\/\\]+)[\/\\]/);
    if (match) {
      const dataset = match[1];
      if (!byDataset[dataset]) byDataset[dataset] = [];
      byDataset[dataset].push(file);
    }
  }
  
  const results = [];
  let worstExitCode = 0;
  
  // Load pipeline config
  const configPath = path.join('config', 'pipeline-config.json');
  let pipelineConfig = {};
  try {
    const configContent = await fs.readFile(configPath, 'utf-8');
    pipelineConfig = JSON.parse(configContent);
  } catch (e) {
    // Config is optional
  }
  
  const datasetNames = Object.keys(byDataset);
  for (let i = 0; i < datasetNames.length; i++) {
    const dataset = datasetNames[i];
    const files = byDataset[dataset];
    const latestFile = files.sort().pop();
    const config = pipelineConfig.datasets?.[dataset.toUpperCase()] || {};
    
    const result = await validateBronze({
      dataset,
      bronzePath: latestFile,
      config,
      format
    });
    
    results.push(result);
    worstExitCode = Math.max(worstExitCode, result.exitCode);
    console.log(`DEBUG validateAllBronze: ${dataset} exitCode=${result.exitCode}, worstExitCode now=${worstExitCode}`);
    
    // Add separator between datasets in pretty mode
    if (format === 'pretty' && i < datasetNames.length - 1) {
      console.log(kleur.gray('─'.repeat(60)));
    }
  }
  
  console.log(`DEBUG validateAllBronze: FINAL worstExitCode=${worstExitCode}`);
  return {
    exitCode: worstExitCode,
    results
  };
}

/**
 * CLI interface
 */
async function main() {
  const args = process.argv.slice(2);
  
  // Parse format flag
  const formatIndex = args.indexOf('--format');
  const format = formatIndex >= 0 ? args[formatIndex + 1] : 'pretty';
  
  if (args.includes('--all')) {
    // Validate all Bronze datasets
    const result = await validateAllBronze({ format });
    process.exit(result.exitCode);
  } else {
    // Parse single dataset validation
    const datasetIndex = args.indexOf('--dataset');
    const bronzeIndex = args.indexOf('--bronze');
    const configIndex = args.indexOf('--config');
    
    if (datasetIndex < 0 || bronzeIndex < 0) {
      console.error('Usage: bronze-validation.js --dataset DATASET --bronze PATH [--config CONFIG] [--format json|pretty]');
      console.error('   or: bronze-validation.js --all [--format json|pretty]');
      process.exit(3);
    }
    
    const dataset = args[datasetIndex + 1];
    const bronzePath = args[bronzeIndex + 1];
    
    // Load config if provided
    let config = {};
    if (configIndex >= 0) {
      const configPath = args[configIndex + 1];
      const configContent = await fs.readFile(configPath, 'utf-8');
      const fullConfig = JSON.parse(configContent);
      config = fullConfig.datasets?.[dataset.toUpperCase()] || {};
    }
    
    // Run validation
    const result = await validateBronze({
      dataset,
      bronzePath,
      config,
      format
    });
    
    // Exit with appropriate code
    process.exit(result.exitCode);
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error(err);
    process.exit(3);
  });
}

module.exports = {
  validateBronze,
  validateAllBronze,
  runExpectations,
  calculateMetrics
};