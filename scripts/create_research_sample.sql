-- Stratified Sampling Strategy for Nevada Purchase Orders Research Dataset
-- Target: 5,000 records that preserve key statistical properties and patterns
-- Methodology: Multi-dimensional stratified sampling with proportional allocation

WITH 
-- Define sampling strata and their weights
strata_definitions AS (
    SELECT 
        -- Temporal sampling: Recent years get more weight
        CASE 
            WHEN fiscal_year >= 2023 THEN 0.35  -- 35% from recent 3 years
            WHEN fiscal_year >= 2020 THEN 0.30  -- 30% from mid-period
            ELSE 0.35                            -- 35% from historical
        END as temporal_weight,
        
        -- Amount-based sampling: Cover full spectrum
        CASE 
            WHEN total_amount < 100 THEN 0.10       -- Small purchases
            WHEN total_amount < 1000 THEN 0.20      -- Routine purchases
            WHEN total_amount < 10000 THEN 0.25     -- Standard purchases
            WHEN total_amount < 100000 THEN 0.25    -- Significant purchases
            WHEN total_amount < 1000000 THEN 0.15   -- Major purchases
            ELSE 0.05                                -- Exceptional purchases
        END as amount_weight,
        
        -- Status sampling: Proportional to actual distribution
        CASE 
            WHEN status_category = 'Sent' THEN 0.44
            WHEN status_category = 'Complete' THEN 0.39
            WHEN status_category = 'Closed' THEN 0.16
            ELSE 0.01  -- Partial and other
        END as status_weight,
        
        -- Vendor importance: Top vendors and long tail
        CASE 
            WHEN vendor_name IN (
                'Amazon Capital Services, Inc.',
                'Staples Advantage',
                'Grainger',
                'Dell Marketing L.P, LLC dba Dell EMC',
                'CDW Government, Inc.'
            ) THEN 0.25  -- Top vendors
            ELSE 0.75     -- Long tail vendors
        END as vendor_weight
    FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
),

-- Add stratification columns and random sampling seed
stratified_data AS (
    SELECT 
        *,
        -- Create composite strata identifier
        fiscal_year || '-' || 
        CASE 
            WHEN total_amount < 100 THEN 'XS'
            WHEN total_amount < 1000 THEN 'S'
            WHEN total_amount < 10000 THEN 'M'
            WHEN total_amount < 100000 THEN 'L'
            WHEN total_amount < 1000000 THEN 'XL'
            ELSE 'XXL'
        END || '-' ||
        status_category AS stratum,
        
        -- Assign random number for sampling within strata
        RANDOM() AS rand_val,
        
        -- Calculate row number within each stratum
        ROW_NUMBER() OVER (
            PARTITION BY 
                fiscal_year,
                CASE 
                    WHEN total_amount < 100 THEN 'XS'
                    WHEN total_amount < 1000 THEN 'S'
                    WHEN total_amount < 10000 THEN 'M'
                    WHEN total_amount < 100000 THEN 'L'
                    WHEN total_amount < 1000000 THEN 'XL'
                    ELSE 'XXL'
                END,
                status_category
            ORDER BY RANDOM()
        ) AS stratum_row_num
    FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
),

-- Calculate sample sizes per stratum
stratum_sizes AS (
    SELECT 
        stratum,
        COUNT(*) AS stratum_total,
        -- Proportional allocation with minimum of 5 samples per stratum
        GREATEST(5, ROUND(5000.0 * COUNT(*) / (SELECT COUNT(*) FROM stratified_data))) AS target_sample_size
    FROM stratified_data
    GROUP BY stratum
),

-- Select samples from each stratum
sampled_data AS (
    SELECT 
        sd.*,
        ss.stratum_total,
        ss.target_sample_size
    FROM stratified_data sd
    JOIN stratum_sizes ss ON sd.stratum = ss.stratum
    WHERE sd.stratum_row_num <= ss.target_sample_size
),

-- Add edge cases and interesting patterns
edge_cases AS (
    SELECT * FROM (
        -- Largest amounts
        SELECT *, 'edge_largest_amount' as sample_reason 
        FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
        ORDER BY total_amount DESC LIMIT 10
        
        UNION ALL
        
        -- Smallest non-zero amounts
        SELECT *, 'edge_smallest_amount' as sample_reason
        FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
        WHERE total_amount > 0
        ORDER BY total_amount ASC LIMIT 10
        
        UNION ALL
        
        -- Most recent orders
        SELECT *, 'edge_most_recent' as sample_reason
        FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
        ORDER BY sent_date DESC LIMIT 20
        
        UNION ALL
        
        -- Oldest orders
        SELECT *, 'edge_oldest' as sample_reason
        FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
        ORDER BY sent_date ASC LIMIT 20
        
        UNION ALL
        
        -- High revision numbers
        SELECT *, 'edge_high_revisions' as sample_reason
        FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
        WHERE revision_number > 100
        ORDER BY revision_number DESC LIMIT 20
    )
),

-- Combine stratified sample with edge cases
final_sample AS (
    SELECT 
        po_id,
        base_po_id,
        revision_number,
        sent_date,
        total_amount,
        description,
        vendor_name,
        organization,
        department,
        buyer_name,
        status,
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        org_code,
        po_fiscal_year,
        status_category,
        'stratified_sample' as sample_method,
        stratum as sample_stratum,
        NULL as edge_case_reason
    FROM sampled_data
    
    UNION
    
    SELECT 
        po_id,
        base_po_id,
        revision_number,
        sent_date,
        total_amount,
        description,
        vendor_name,
        organization,
        department,
        buyer_name,
        status,
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        org_code,
        po_fiscal_year,
        status_category,
        'edge_case' as sample_method,
        NULL as sample_stratum,
        sample_reason as edge_case_reason
    FROM edge_cases
)

-- Final output with deduplication
SELECT DISTINCT ON (po_id) 
    *
FROM final_sample
ORDER BY po_id, sample_method
LIMIT 5000;