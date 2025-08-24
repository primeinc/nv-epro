#!/usr/bin/env node

const { DuckDBInstance, DuckDBConnection } = require('@duckdb/node-api');
const fs = require('fs').promises;
const path = require('path');

async function generateSample(connection, tableName, transformFile, targetSize) {
    console.log(`\nGenerating ${tableName} sample...`);
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    const outputDir = path.join(process.cwd(), 'data', 'research', tableName);
    
    // Create output directory
    await fs.mkdir(outputDir, { recursive: true });
    
    // Read transform SQL
    const sql = await fs.readFile(transformFile, 'utf-8');
    
    // Create view and export files
    const viewName = `${tableName}_sample`;
    const parquetPath = path.join(outputDir, `${tableName}_sample_${timestamp}.parquet`);
    const csvPath = path.join(outputDir, `${tableName}_sample_${timestamp}.csv`);
    const jsonlPath = path.join(outputDir, `${tableName}_sample_${timestamp}.jsonl`);
    const metadataPath = path.join(outputDir, `${tableName}_sample_${timestamp}_metadata.json`);
    
    try {
        // Create temporary view
        await connection.run(`CREATE OR REPLACE TEMPORARY VIEW ${viewName} AS ${sql}`);
        
        // Export to different formats
        console.log(`  Exporting to Parquet...`);
        await connection.run(`COPY ${viewName} TO '${parquetPath}' (FORMAT PARQUET);`);
        
        console.log(`  Exporting to CSV...`);
        await connection.run(`COPY ${viewName} TO '${csvPath}' (FORMAT CSV, HEADER);`);
        
        console.log(`  Exporting to JSON Lines...`);
        await connection.run(`COPY ${viewName} TO '${jsonlPath}' (FORMAT JSON);`);
        
        // Generate statistics using runAndReadAll
        const statsReader = await connection.runAndReadAll(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ${getKeyColumn(tableName)}) as unique_records
            FROM ${viewName}
        `);
        const statsRows = statsReader.getRows();
        const stats = {
            total_records: statsRows[0][0],
            unique_records: statsRows[0][1]
        };
        
        // Get column info
        const columnsReader = await connection.runAndReadAll(`
            SELECT column_name, column_type 
            FROM (DESCRIBE SELECT * FROM ${viewName})
        `);
        const columnsResult = columnsReader.getRows();
        
        // Create metadata
        const metadata = {
            generated_at: new Date().toISOString(),
            table: tableName,
            version: 'v1.0.0',
            transform_file: path.basename(transformFile),
            target_size: targetSize,
            actual_size: Number(stats.total_records),
            unique_records: Number(stats.unique_records),
            columns: columnsResult.map(row => ({
                name: row[0],  // column_name
                type: row[1]   // column_type
            })),
            output_files: {
                parquet: path.basename(parquetPath),
                csv: path.basename(csvPath),
                jsonl: path.basename(jsonlPath)
            }
        };
        
        await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
        
        // Get file sizes
        const [parquetStat, csvStat, jsonlStat] = await Promise.all([
            fs.stat(parquetPath),
            fs.stat(csvPath),
            fs.stat(jsonlPath)
        ]);
        
        console.log(`  ✓ Generated ${metadata.actual_size} records`);
        console.log(`    - Parquet: ${(parquetStat.size / 1024).toFixed(1)} KB`);
        console.log(`    - CSV: ${(csvStat.size / 1024).toFixed(1)} KB`);
        console.log(`    - JSON Lines: ${(jsonlStat.size / 1024).toFixed(1)} KB`);
        
        // Clean up old timestamped files (keep only latest)
        const files = await fs.readdir(outputDir);
        const timestampPattern = /(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})/;
        for (const file of files) {
            const match = file.match(timestampPattern);
            if (match && match[1] !== timestamp && file.includes(tableName)) {
                try {
                    await fs.unlink(path.join(outputDir, file));
                    console.log(`  Removed old file: ${file}`);
                } catch (e) {
                    // Ignore errors
                }
            }
        }
        
        // Create symlinks to latest
        const latestParquet = path.join(outputDir, `${tableName}_sample.parquet`);
        const latestCsv = path.join(outputDir, `${tableName}_sample.csv`);
        const latestJsonl = path.join(outputDir, `${tableName}_sample.jsonl`);
        
        // Remove old symlinks if they exist
        for (const link of [latestParquet, latestCsv, latestJsonl]) {
            try {
                await fs.unlink(link);
            } catch (e) {
                // Ignore if doesn't exist
            }
        }
        
        // Create new symlinks (on Windows, copy instead)
        if (process.platform === 'win32') {
            await fs.copyFile(parquetPath, latestParquet);
            await fs.copyFile(csvPath, latestCsv);
            await fs.copyFile(jsonlPath, latestJsonl);
        } else {
            await fs.symlink(path.basename(parquetPath), latestParquet);
            await fs.symlink(path.basename(csvPath), latestCsv);
            await fs.symlink(path.basename(jsonlPath), latestJsonl);
        }
        
        return metadata;
        
    } catch (error) {
        console.error(`  ✗ Error generating ${tableName} sample:`, error.message);
        throw error;
    }
}

function getKeyColumn(tableName) {
    switch(tableName) {
        case 'bids': return 'bid_solicitation_id';
        case 'contracts': return 'contract_id';
        case 'vendors': return 'vendor_id';
        case 'purchase_orders': return 'po_id';
        default: return '*';
    }
}

async function generateReadme(outputDir, samples) {
    const readme = `# Nevada Research Sample Datasets

Generated: ${new Date().toISOString()}

## Overview
This directory contains carefully curated research samples from Nevada's procurement data, designed for analysis by LLMs and data scientists.

## Datasets

### Bids Sample
- **Records**: ${samples.bids.actual_size} (from 2,674 total)
- **Coverage**: ${(samples.bids.actual_size / 2674 * 100).toFixed(1)}% of all bids
- **Key Features**: Stratified by fiscal year, organization tier, contract status
- **Use Cases**: Bid pattern analysis, vendor competition studies, procurement timeline analysis

### Contracts Sample
- **Records**: ${samples.contracts.actual_size} (from 1,607 total)
- **Coverage**: ${(samples.contracts.actual_size / 1607 * 100).toFixed(1)}% of all contracts
- **Key Features**: Balanced across spend ranges, includes all mega contracts
- **Use Cases**: Spend analysis, vendor relationship studies, contract duration patterns

### Vendors Sample
- **Records**: ${samples.vendors.actual_size} (from 19,573 total)
- **Coverage**: ${(samples.vendors.actual_size / 19573 * 100).toFixed(1)}% of all vendors
- **Key Features**: All major vendors, diverse business types, entity classification
- **Use Cases**: Vendor diversity analysis, business type studies, market competition

### Purchase Orders Sample
- **Records**: ~5,000 (from 96,985 total)
- **Coverage**: 5.2% of all purchase orders
- **Key Features**: Multi-dimensional stratification, edge cases included
- **Use Cases**: Spending pattern analysis, vendor concentration, temporal trends

## File Formats
Each dataset is available in three formats:
- **.parquet** - Most efficient for analysis tools (DuckDB, Pandas, Spark)
- **.csv** - Human-readable, compatible with Excel and basic tools
- **.jsonl** - JSON Lines format for streaming processing

## Usage Examples

### Python/Pandas
\`\`\`python
import pandas as pd

# Load any dataset
bids_df = pd.read_parquet('bids/bids_sample.parquet')
contracts_df = pd.read_parquet('contracts/contracts_sample.parquet')
vendors_df = pd.read_parquet('vendors/vendors_sample.parquet')
pos_df = pd.read_parquet('research_sample_*.parquet')  # Latest PO sample

# Analyze relationships
merged = bids_df.merge(contracts_df, on='contract_id', how='left')
print(f"Bids with contracts: {merged['contract_id'].notna().sum()}")
\`\`\`

### DuckDB SQL
\`\`\`sql
-- Load and join datasets
SELECT 
    b.fiscal_year,
    b.organization,
    COUNT(DISTINCT b.bid_solicitation_id) as bid_count,
    COUNT(DISTINCT c.contract_id) as contract_count,
    SUM(c.dollars_spent_to_date) as total_spend
FROM 'bids/bids_sample.parquet' b
LEFT JOIN 'contracts/contracts_sample.parquet' c
    ON b.contract_id = c.contract_id
GROUP BY b.fiscal_year, b.organization
ORDER BY total_spend DESC;
\`\`\`

### R
\`\`\`r
library(arrow)
library(dplyr)

# Load datasets
bids <- read_parquet('bids/bids_sample.parquet')
contracts <- read_parquet('contracts/contracts_sample.parquet')
vendors <- read_parquet('vendors/vendors_sample.parquet')

# Analyze vendor distribution
vendor_summary <- vendors %>%
    group_by(vendor_type) %>%
    summarise(
        count = n(),
        pct = n() / nrow(vendors) * 100
    ) %>%
    arrange(desc(count))
\`\`\`

## Sampling Methodology
All samples use stratified sampling to maintain statistical representativeness:
- **Temporal stratification**: Recent, mid-period, and historical records
- **Size stratification**: Different tiers based on amounts or organizational size
- **Category stratification**: Ensuring all important categories are represented
- **Edge case inclusion**: Extreme values and anomalies for robustness testing

## Notes
- These samples are designed for research and analysis purposes
- Vendor location data is not available in the source data
- All financial amounts are in USD
- Dates use ISO 8601 format (YYYY-MM-DD)
`;

    await fs.writeFile(path.join(outputDir, 'README.md'), readme);
}

async function createRootSymlinks(researchDir) {
    // Create symlinks/copies of all CSV files in the research root
    const tables = ['bids', 'contracts', 'vendors', 'purchase_orders'];
    
    for (const table of tables) {
        const sourceFile = path.join(researchDir, table, `${table}_sample.csv`);
        const targetFile = path.join(researchDir, `${table}_sample.csv`);
        
        try {
            // Remove old file if exists
            try {
                await fs.unlink(targetFile);
            } catch (e) {
                // Ignore if doesn't exist
            }
            
            // Check if source exists
            await fs.access(sourceFile);
            
            // Copy to root (symlinks don't work well on Windows)
            await fs.copyFile(sourceFile, targetFile);
            console.log(`  Created root CSV: ${table}_sample.csv`);
        } catch (e) {
            console.log(`  Skipping root CSV for ${table}: ${e.message}`);
        }
    }
}

async function main() {
    console.log('Generating research samples for all tables...');
    
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
        const samples = {};
        
        // Generate bids sample
        samples.bids = await generateSample(
            connection,
            'bids',
            path.join(process.cwd(), 'transforms', 'research_sample_bids_v1.0.0.sql'),
            1000
        );
        
        // Generate contracts sample
        samples.contracts = await generateSample(
            connection,
            'contracts',
            path.join(process.cwd(), 'transforms', 'research_sample_contracts_v1.0.0.sql'),
            800
        );
        
        // Generate vendors sample
        samples.vendors = await generateSample(
            connection,
            'vendors',
            path.join(process.cwd(), 'transforms', 'research_sample_vendors_v1.0.0.sql'),
            2000
        );
        
        // Generate purchase_orders sample
        samples.purchase_orders = await generateSample(
            connection,
            'purchase_orders',
            path.join(process.cwd(), 'transforms', 'research_sample_v1.0.0.sql'),
            5000
        );
        
        // Generate combined README
        const researchDir = path.join(process.cwd(), 'data', 'research');
        await generateReadme(researchDir, samples);
        
        // Create root-level CSV symlinks/copies
        console.log('\nCreating root-level CSV files...');
        await createRootSymlinks(researchDir);
        
        console.log('\n✅ All research samples generated successfully!');
        console.log(`📁 Output directory: ${researchDir}`);
        
    } catch (error) {
        console.error('\n❌ Error generating samples:', error);
        process.exit(1);
    } finally {
        // DuckDB Node API doesn't have close methods, connections are managed automatically
    }
}

if (require.main === module) {
    main().catch(console.error);
}