#!/usr/bin/env node

/**
 * Silver Layer Ingestion
 * 
 * Applies SQL transformations to Bronze data to create Silver layer
 * - Normalized schemas
 * - Type casting
 * - Business logic filters
 * - Temporal tracking
 */

const path = require('path');
const fs = require('fs').promises;
const duckdb = require('@duckdb/node-api');
const crypto = require('crypto');

/**
 * Generate snapshot ID for this Silver run
 */
function generateSnapshotId() {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const hash = crypto.randomBytes(4).toString('hex');
  return `snapshot_${timestamp}_${hash}`;
}

/**
 * Apply Silver transformation
 */
async function transformToSilver(dataset, options = {}) {
  const { 
    bronzeBasePath = 'data/bronze',
    silverBasePath = 'data/silver',
    transformVersion = null  // Will auto-detect latest
  } = options;
  
  console.log(`\n📊 Transforming ${dataset} to Silver...`);
  
  // Auto-detect latest transform version if not specified
  const fg = require('fast-glob');
  let actualVersion = transformVersion;
  if (!actualVersion) {
    const transforms = await fg(`transforms/silver/${dataset}_v*.sql`);
    if (transforms.length === 0) {
      console.error(`❌ No Silver transforms found for ${dataset}`);
      return { success: false, error: `No transforms for ${dataset}` };
    }
    // Get the latest version
    transforms.sort();
    const latestPath = transforms[transforms.length - 1];
    actualVersion = path.basename(latestPath).match(/_v(\d+\.\d+\.\d+)\.sql$/)[1];
    actualVersion = `v${actualVersion}`;
  }
  
  // Load transformation SQL
  const sqlPath = path.join('transforms', 'silver', `${dataset}_${actualVersion}.sql`);
  let transformSql;
  try {
    transformSql = await fs.readFile(sqlPath, 'utf-8');
  } catch (error) {
    console.error(`❌ No Silver transform found: ${sqlPath}`);
    return { success: false, error: `Missing transform: ${sqlPath}` };
  }
  
  // Find latest Bronze data
  const bronzePattern = path.join(bronzeBasePath, dataset, '**/data.parquet').replace(/\\/g, '/');
  const bronzeFiles = await fg(bronzePattern);
  
  if (bronzeFiles.length === 0) {
    console.error(`❌ No Bronze data found for ${dataset}`);
    return { success: false, error: 'No Bronze data' };
  }
  
  console.log(`   Found ${bronzeFiles.length} Bronze file(s)`);
  
  // Connect to DuckDB
  const conn = await duckdb.DuckDBConnection.create();
  
  try {
    // Create view of all Bronze data
    const bronzeViewSql = `
      CREATE OR REPLACE VIEW bronze_${dataset} AS
      SELECT * FROM read_parquet([${bronzeFiles.map(f => `'${f}'`).join(', ')}])
    `;
    await conn.run(bronzeViewSql);
    
    // Get row count for validation
    const countReader = await conn.runAndReadAll(`SELECT COUNT(*) FROM bronze_${dataset}`);
    const bronzeRowCount = Number(countReader.getRows()[0][0]);
    console.log(`   Bronze rows: ${bronzeRowCount.toLocaleString()}`);
    
    // Replace placeholders in SQL
    const snapshotId = generateSnapshotId();
    const finalSql = transformSql
      .replace(/{bronze_table}/g, `bronze_${dataset}`)
      .replace(/{transform_version}/g, actualVersion)
      .replace(/{snapshot_id}/g, snapshotId);
    
    // Apply transformation
    console.log(`   Applying ${actualVersion} transformation...`);
    const transformView = `CREATE OR REPLACE VIEW silver_${dataset} AS ${finalSql}`;
    await conn.run(transformView);
    
    // Get Silver row count
    const silverCountReader = await conn.runAndReadAll(`SELECT COUNT(*) FROM silver_${dataset}`);
    const silverRowCount = Number(silverCountReader.getRows()[0][0]);
    console.log(`   Silver rows: ${silverRowCount.toLocaleString()} (filtered: ${bronzeRowCount - silverRowCount})`);
    
    // Write to Parquet
    const silverPath = path.join(
      silverBasePath,
      dataset,
      `version=${actualVersion}`,
      `snapshot=${snapshotId}`,
      'data.parquet'
    );
    
    await fs.mkdir(path.dirname(silverPath), { recursive: true });
    
    const exportSql = `
      COPY silver_${dataset}
      TO '${silverPath.replace(/\\/g, '/')}'
      WITH (FORMAT PARQUET, CODEC 'ZSTD', ROW_GROUP_SIZE 100000)
    `;
    
    await conn.run(exportSql);
    console.log(`   ✅ Written to: ${silverPath}`);
    
    // Create manifest
    const manifest = {
      dataset,
      snapshot_id: snapshotId,
      transform_version: actualVersion,
      transform_sql: sqlPath,
      bronze_files: bronzeFiles.length,
      bronze_rows: bronzeRowCount,
      silver_rows: silverRowCount,
      filtered_rows: bronzeRowCount - silverRowCount,
      created_at: new Date().toISOString(),
      silver_path: silverPath
    };
    
    const manifestPath = path.join(path.dirname(silverPath), 'manifest.json');
    await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
    
    // Validation summary
    if (silverRowCount === 0) {
      console.warn(`   ⚠️  Warning: All rows filtered out!`);
    } else {
      const filterRate = ((bronzeRowCount - silverRowCount) / bronzeRowCount * 100).toFixed(1);
      console.log(`   📈 Filter rate: ${filterRate}%`);
    }
    
    conn.disconnectSync();
    
    return {
      success: true,
      ...manifest
    };
    
  } catch (error) {
    conn.disconnectSync();
    console.error(`❌ Transform failed: ${error.message}`);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Transform all datasets to Silver
 */
async function transformAllToSilver() {
  const datasets = ['bids', 'purchase_orders', 'contracts', 'vendors'];
  const results = [];
  
  console.log('\n=== [scripts/silver-ingest.js] ===');
  console.log('🚀 Starting Silver layer transformation\n');
  console.log('=' .repeat(60));
  
  for (const dataset of datasets) {
    const result = await transformToSilver(dataset);
    results.push({ dataset, ...result });
  }
  
  // Summary
  console.log('\n' + '=' .repeat(60));
  console.log('📈 Silver Transformation Summary\n');
  
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  
  console.log(`✅ Successful: ${successful.length} datasets`);
  if (failed.length > 0) {
    console.log(`❌ Failed: ${failed.length} datasets`);
  }
  
  console.log('\nDetails:');
  results.forEach(r => {
    if (r.success) {
      const filterPct = ((r.filtered_rows / r.bronze_rows) * 100).toFixed(1);
      console.log(`  ${r.dataset}: ${r.silver_rows} rows (${filterPct}% filtered)`);
    } else {
      console.log(`  ${r.dataset}: Failed - ${r.error}`);
    }
  });
  
  return results;
}

// Run if called directly
if (require.main === module) {
  transformAllToSilver().catch(console.error);
}

module.exports = {
  transformToSilver,
  transformAllToSilver
};