#!/usr/bin/env node

const fs = require('fs').promises;
const path = require('path');
const { DuckDBInstance, DuckDBConnection } = require('@duckdb/node-api');

async function generateResearchSample() {
    console.log('🔬 Generating Research Sample Dataset...');
    console.log('=====================================\n');
    
    const transformPath = path.join(__dirname, '..', 'transforms', 'research_sample_v1.0.0.sql');
    const outputDir = path.join(__dirname, '..', 'data', 'research');
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
        // Ensure output directory exists
        await fs.mkdir(outputDir, { recursive: true });
        
        // Read the transform SQL
        console.log('📖 Reading transform SQL...');
        const sql = await fs.readFile(transformPath, 'utf8');
        
        // Generate timestamp for file naming
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
        
        console.log('⚙️  Executing stratified sampling transform...');
        
        // Execute the transform and create a temporary view
        await connection.run(sql);
        await connection.run(`CREATE TEMPORARY VIEW research_sample AS ${sql}`);
        
        // Export to Parquet
        console.log('\n📦 Exporting to Parquet format...');
        const parquetPath = path.join(outputDir, `research_sample_${timestamp}.parquet`);
        await connection.run(`COPY research_sample TO '${parquetPath}' (FORMAT PARQUET);`);
        console.log('   ✓ Parquet export complete');
        
        // Export to CSV  
        console.log('\n📝 Exporting to CSV format...');
        const csvPath = path.join(outputDir, `research_sample_${timestamp}.csv`);
        await connection.run(`COPY research_sample TO '${csvPath}' (FORMAT CSV, HEADER);`);
        console.log('   ✓ CSV export complete');
        
        // Export to JSON Lines
        console.log('\n📋 Exporting to JSON Lines format...');
        const jsonlPath = path.join(outputDir, `research_sample_${timestamp}.jsonl`);
        await connection.run(`COPY research_sample TO '${jsonlPath}' (FORMAT JSON);`);
        console.log('   ✓ JSON Lines export complete');
        
        // Get statistics about the sample
        console.log('\n📊 Generating sample statistics...');
        const statsReader = await connection.runAndReadAll(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT fiscal_year) as unique_years,
                COUNT(DISTINCT vendor_name) as unique_vendors,
                COUNT(DISTINCT organization) as unique_orgs,
                MIN(total_amount) as min_amount,
                MAX(total_amount) as max_amount,
                ROUND(AVG(total_amount), 2) as avg_amount,
                MEDIAN(total_amount) as median_amount
            FROM research_sample
        `);
        const statsRows = statsReader.getRowObjects();
        const stats = statsRows[0];
        
        // Convert BigInt values to strings for JSON serialization
        Object.keys(stats).forEach(key => {
            if (typeof stats[key] === 'bigint') {
                stats[key] = stats[key].toString();
            }
        });
        
        console.log('\nSample Statistics:');
        console.log(`  Total Records: ${stats.total_records}`);
        console.log(`  Unique Years: ${stats.unique_years}`);
        console.log(`  Unique Vendors: ${stats.unique_vendors}`);
        console.log(`  Unique Organizations: ${stats.unique_orgs}`);
        console.log(`  Amount Range: $${stats.min_amount} - $${stats.max_amount}`);
        console.log(`  Average Amount: $${stats.avg_amount}`);
        console.log(`  Median Amount: $${stats.median_amount}`);
        
        // Get distribution by key dimensions
        console.log('\n📈 Analyzing sample distribution...');
        
        // Check fiscal year distribution
        const yearReader = await connection.runAndReadAll(`
            SELECT 
                fiscal_year,
                COUNT(*) as count
            FROM research_sample
            GROUP BY fiscal_year
            ORDER BY fiscal_year
        `);
        const yearDist = yearReader.getRowObjects().map(row => ({
            ...row,
            fiscal_year: typeof row.fiscal_year === 'bigint' ? row.fiscal_year.toString() : row.fiscal_year,
            count: typeof row.count === 'bigint' ? row.count.toString() : row.count
        }));
        
        console.log('\nDistribution by Year:');
        yearDist.forEach(row => {
            console.log(`  ${row.fiscal_year}: ${row.count} records`);
        });
        
        // Check status distribution
        const statusReader = await connection.runAndReadAll(`
            SELECT 
                status_category,
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
            FROM research_sample
            GROUP BY status_category
            ORDER BY count DESC
        `);
        const statusDist = statusReader.getRowObjects().map(row => ({
            ...row,
            count: typeof row.count === 'bigint' ? row.count.toString() : row.count,
            pct: typeof row.pct === 'bigint' ? row.pct.toString() : row.pct
        }));
        
        console.log('\nDistribution by Status:');
        statusDist.forEach(row => {
            console.log(`  ${row.status_category}: ${row.count} records (${row.pct}%)`);
        });
        
        // Check amount range distribution (if column exists)
        try {
            const amountReader = await connection.runAndReadAll(`
                SELECT 
                    amount_range,
                    COUNT(*) as count,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
                FROM research_sample
                GROUP BY amount_range
                ORDER BY 
                    CASE amount_range
                        WHEN '$0' THEN 1
                        WHEN '<$100' THEN 2
                        WHEN '$100-1K' THEN 3
                        WHEN '$1K-10K' THEN 4
                        WHEN '$10K-100K' THEN 5
                        WHEN '$100K-1M' THEN 6
                        WHEN '>$1M' THEN 7
                    END
            `);
            const amountDist = amountReader.getRowObjects().map(row => ({
                ...row,
                count: typeof row.count === 'bigint' ? row.count.toString() : row.count,
                pct: typeof row.pct === 'bigint' ? row.pct.toString() : row.pct
            }));
            
            console.log('\nDistribution by Amount Range:');
            amountDist.forEach(row => {
                console.log(`  ${row.amount_range}: ${row.count} records (${row.pct}%)`);
            });
        } catch {
            // amount_range column might not exist
        }
        
        // Create metadata file
        console.log('\n📄 Creating metadata file...');
        const metadata = {
            generated_at: new Date().toISOString(),
            version: 'v1.0.0',
            transform_file: 'research_sample_v1.0.0.sql',
            sampling_method: 'Stratified sampling with edge cases',
            target_size: 5000,
            actual_size: stats.total_records,
            statistics: {
                unique_years: stats.unique_years,
                unique_vendors: stats.unique_vendors,
                unique_organizations: stats.unique_orgs,
                min_amount: parseFloat(stats.min_amount),
                max_amount: parseFloat(stats.max_amount),
                avg_amount: parseFloat(stats.avg_amount),
                median_amount: parseFloat(stats.median_amount)
            },
            distributions: {
                by_year: yearDist,
                by_status: statusDist
            },
            dimensions: {
                temporal: ['recent (2023+)', 'mid (2020-2022)', 'historical (pre-2020)'],
                amount: ['$0', '<$100', '$100-1K', '$1K-10K', '$10K-100K', '$100K-1M', '>$1M'],
                vendor_tier: ['top10', 'top50', 'longtail'],
                org_size: ['large_org', 'other_org'],
                status: ['Sent', 'Complete', 'Closed', 'Partial']
            },
            edge_cases_included: [
                'extreme_high_amount',
                'zero_amount',
                'tiny_amount',
                'high_revisions',
                'most_recent',
                'oldest',
                'unusual_partial',
                'long_description'
            ],
            output_files: {
                parquet: path.basename(parquetPath),
                csv: path.basename(csvPath),
                jsonl: path.basename(jsonlPath)
            }
        };
        
        const metadataPath = path.join(outputDir, `research_sample_${timestamp}_metadata.json`);
        await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
        
        // Create README
        console.log('📚 Creating README...');
        const readme = `# Nevada Purchase Orders Research Sample

Generated: ${new Date().toISOString()}

## Overview
This is a carefully curated sample of ~5,000 Nevada purchase orders designed for research and analysis by LLMs and data scientists.

## Statistics
- **Total Records**: ${stats.total_records}
- **Temporal Range**: ${stats.unique_years} fiscal years
- **Vendor Diversity**: ${stats.unique_vendors} unique vendors
- **Organization Coverage**: ${stats.unique_orgs} organizations
- **Amount Range**: $${stats.min_amount} - $${stats.max_amount}

## Sampling Methodology
- **Stratified Sampling**: Multi-dimensional stratification across time, amount, vendor, and organization dimensions
- **Edge Cases**: Includes extreme values, anomalies, and interesting patterns
- **Proportional Allocation**: Sample sizes per stratum proportional to population with minimum thresholds

## Files
- \`${path.basename(parquetPath)}\` - Parquet format (most efficient for analysis)
- \`${path.basename(csvPath)}\` - CSV format (human-readable, Excel-compatible)
- \`${path.basename(jsonlPath)}\` - JSON Lines format (streaming-friendly)
- \`${path.basename(metadataPath)}\` - Detailed metadata about the sample

## Usage Examples

### Python/Pandas
\`\`\`python
import pandas as pd
df = pd.read_parquet('${path.basename(parquetPath)}')

# Basic analysis
print(df.info())
print(df.describe())

# Group by fiscal year
yearly = df.groupby('fiscal_year')['total_amount'].agg(['count', 'mean', 'sum'])
\`\`\`

### DuckDB
\`\`\`sql
-- Load and explore
SELECT * FROM '${path.basename(parquetPath)}' LIMIT 10;

-- Aggregate by vendor
SELECT 
    vendor_name,
    COUNT(*) as po_count,
    SUM(total_amount) as total_spend
FROM '${path.basename(parquetPath)}'
GROUP BY vendor_name
ORDER BY total_spend DESC
LIMIT 10;
\`\`\`

### R
\`\`\`r
library(arrow)
df <- read_parquet('${path.basename(parquetPath)}')

# Summary statistics
summary(df)

# Visualize
library(ggplot2)
ggplot(df, aes(x = fiscal_year, y = total_amount)) +
    geom_boxplot() +
    scale_y_log10()
\`\`\`
`;
        
        const readmePath = path.join(outputDir, 'README.md');
        await fs.writeFile(readmePath, readme);
        
        console.log('\n✅ Research sample generation complete!');
        console.log(`📁 Output directory: ${outputDir}`);
        console.log('\nGenerated files:');
        console.log(`  - ${path.basename(parquetPath)} (Parquet format)`);
        console.log(`  - ${path.basename(csvPath)} (CSV format)`);
        console.log(`  - ${path.basename(jsonlPath)} (JSON Lines format)`);
        console.log(`  - ${path.basename(metadataPath)} (Metadata)`);
        console.log(`  - README.md (Documentation)`);
        
    } catch (error) {
        console.error('\n❌ Error generating research sample:', error.message);
        console.error(error.stack);
        process.exit(1);
    } finally {
        // Clean up database connection
        connection.disconnectSync();
    }
}

// Run if called directly
if (require.main === module) {
    generateResearchSample();
}

module.exports = { generateResearchSample };