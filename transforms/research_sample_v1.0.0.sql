-- Research Sample Dataset Generator v1.0.0
-- Purpose: Create a stratified sample of ~5000 Nevada purchase orders for research/analysis
-- Target: Balance between statistical representation and manageable size for LLM analysis
-- Method: Multi-dimensional stratified sampling with edge case inclusion

WITH 
-- Step 0: Pre-calculate vendor and org rankings to avoid hardcoding
vendor_ranks AS (
    SELECT 
        vendor_name,
        COUNT(*) as vendor_po_count,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as vendor_rank
    FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
    GROUP BY vendor_name
),
org_ranks AS (
    SELECT 
        organization,
        COUNT(*) as org_po_count,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as org_rank
    FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet'
    GROUP BY organization
),

-- Step 1: Annotate all records with stratification dimensions
base_data AS (
    SELECT 
        p.*,
        -- Temporal dimension
        CASE 
            WHEN p.fiscal_year >= 2023 THEN 'recent'      -- Last 3 years
            WHEN p.fiscal_year >= 2020 THEN 'mid'         -- COVID era
            ELSE 'historical'                             -- Pre-2020
        END as temporal_group,
        
        -- Amount dimension with business-meaningful ranges
        CASE 
            WHEN p.total_amount = 0 THEN 'zero'
            WHEN p.total_amount < 100 THEN 'micro'        -- Petty cash level
            WHEN p.total_amount < 1000 THEN 'small'       -- Routine supplies
            WHEN p.total_amount < 10000 THEN 'medium'     -- Standard purchases
            WHEN p.total_amount < 100000 THEN 'large'     -- Significant contracts
            WHEN p.total_amount < 1000000 THEN 'major'    -- Major investments
            ELSE 'mega'                                   -- Exceptional deals
        END as amount_group,
        
        -- Dynamic vendor concentration based on actual data
        CASE 
            WHEN vr.vendor_rank <= 10 THEN 'top10'
            WHEN vr.vendor_rank <= 50 THEN 'top50'
            ELSE 'longtail'
        END as vendor_tier,
        
        -- Dynamic organization size based on actual data
        CASE 
            WHEN org_r.org_rank <= 5 THEN 'large_org'
            ELSE 'other_org'
        END as org_size
        
    FROM 'data/silver/purchase_orders/version=v0.5.0/snapshot=*/data.parquet' p
    LEFT JOIN vendor_ranks vr ON p.vendor_name = vr.vendor_name
    LEFT JOIN org_ranks org_r ON p.organization = org_r.organization
),

-- Step 2: Create composite strata and calculate proportions
stratified_data AS (
    SELECT 
        *,
        -- Create composite stratum identifier
        temporal_group || '|' || 
        amount_group || '|' || 
        status_category || '|' ||
        vendor_tier || '|' ||
        org_size AS stratum,
        
        -- Add random value for sampling
        RANDOM() AS rand_val,
        
        -- Row number within each stratum for sampling
        ROW_NUMBER() OVER (
            PARTITION BY 
                temporal_group,
                amount_group,
                status_category,
                vendor_tier,
                org_size
            ORDER BY RANDOM()
        ) AS stratum_row
    FROM base_data
),

-- Step 3: Calculate sample sizes per stratum (proportional allocation)
stratum_allocations AS (
    SELECT 
        stratum,
        COUNT(*) AS stratum_size,
        COUNT(*) * 1.0 / (SELECT COUNT(*) FROM base_data) AS stratum_proportion,
        -- Proportional allocation with minimum 2 samples per non-empty stratum
        GREATEST(
            2, 
            ROUND(4800.0 * COUNT(*) / (SELECT COUNT(*) FROM base_data))
        ) AS target_samples
    FROM stratified_data
    GROUP BY stratum
),

-- Step 4: Select stratified sample
stratified_sample AS (
    SELECT 
        sd.*,
        'stratified' as sampling_method,
        sa.stratum_size,
        sa.stratum_proportion,
        sa.target_samples
    FROM stratified_data sd
    JOIN stratum_allocations sa ON sd.stratum = sa.stratum
    WHERE sd.stratum_row <= sa.target_samples
),

-- Step 5: Add edge cases and interesting patterns (200 records reserved)
edge_cases AS (
    -- Extreme amounts
    SELECT *, 'extreme_high_amount' as edge_type FROM (
        SELECT * FROM base_data 
        ORDER BY total_amount DESC 
        LIMIT 20
    )
    UNION ALL
    -- Zero amounts
    SELECT *, 'zero_amount' as edge_type FROM (
        SELECT * FROM base_data 
        WHERE total_amount = 0 
        ORDER BY RANDOM() 
        LIMIT 20
    )
    UNION ALL
    -- Very small non-zero
    SELECT *, 'tiny_amount' as edge_type FROM (
        SELECT * FROM base_data 
        WHERE total_amount > 0 AND total_amount < 10 
        ORDER BY RANDOM() 
        LIMIT 20
    )
    UNION ALL
    -- High revision numbers
    SELECT *, 'high_revisions' as edge_type FROM (
        SELECT * FROM base_data 
        WHERE revision_number > 100 
        ORDER BY revision_number DESC 
        LIMIT 30
    )
    UNION ALL
    -- Recent dates
    SELECT *, 'most_recent' as edge_type FROM (
        SELECT * FROM base_data 
        ORDER BY sent_date DESC 
        LIMIT 30
    )
    UNION ALL
    -- Historical dates
    SELECT *, 'oldest' as edge_type FROM (
        SELECT * FROM base_data 
        ORDER BY sent_date ASC 
        LIMIT 30
    )
    UNION ALL
    -- Unusual patterns (partial status with high amounts)
    SELECT *, 'unusual_partial' as edge_type FROM (
        SELECT * FROM base_data 
        WHERE status_category = 'Partial' AND total_amount > 10000
        ORDER BY total_amount DESC 
        LIMIT 30
    )
    UNION ALL
    -- Long descriptions
    SELECT *, 'long_description' as edge_type FROM (
        SELECT * FROM base_data 
        WHERE LENGTH(description) > 200
        ORDER BY LENGTH(description) DESC 
        LIMIT 20
    )
),

-- Step 6: Combine and deduplicate
combined_sample AS (
    SELECT DISTINCT ON (po_id)
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
        has_valid_amount,
        has_valid_date,
        -- Sampling metadata
        temporal_group,
        amount_group,
        vendor_tier,
        org_size,
        stratum,
        sampling_method,
        NULL as edge_type
    FROM stratified_sample
    
    UNION ALL
    
    SELECT DISTINCT ON (po_id)
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
        has_valid_amount,
        has_valid_date,
        -- Sampling metadata
        temporal_group,
        amount_group,
        vendor_tier,
        org_size,
        temporal_group || '|' || amount_group || '|' || status_category || '|' || vendor_tier || '|' || org_size as stratum,
        'edge_case' as sampling_method,
        edge_type
    FROM edge_cases
)

-- Final selection with enriched metadata
SELECT 
    -- Core fields from silver layer
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
    
    -- Analytical dimensions from stratification
    temporal_group,
    amount_group,
    vendor_tier,
    org_size,
    
    -- Computed features for analysis
    EXTRACT(YEAR FROM sent_date) as year,
    EXTRACT(MONTH FROM sent_date) as month,
    EXTRACT(DOW FROM sent_date) as day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM sent_date) IN (0, 6) THEN 'weekend'
        ELSE 'weekday'
    END as day_type,
    LENGTH(description) as description_length,
    CASE 
        WHEN LOWER(description) LIKE '%covid%' OR LOWER(description) LIKE '%pandemic%' THEN true
        ELSE false
    END as covid_related,
    CASE 
        WHEN LOWER(description) LIKE '%emergency%' OR LOWER(description) LIKE '%urgent%' THEN true
        ELSE false
    END as emergency_purchase,
    
    -- Sampling metadata
    sampling_method,
    stratum as sample_stratum,
    edge_type,
    
    -- Amount categorization for easy filtering
    CASE 
        WHEN total_amount = 0 THEN '$0'
        WHEN total_amount < 100 THEN '<$100'
        WHEN total_amount < 1000 THEN '$100-1K'
        WHEN total_amount < 10000 THEN '$1K-10K'
        WHEN total_amount < 100000 THEN '$10K-100K'
        WHEN total_amount < 1000000 THEN '$100K-1M'
        ELSE '>$1M'
    END as amount_range,
    
    -- Revision analysis
    CASE 
        WHEN revision_number = 0 THEN 'original'
        WHEN revision_number <= 5 THEN 'low_revision'
        WHEN revision_number <= 20 THEN 'moderate_revision'
        ELSE 'high_revision'
    END as revision_category
    
FROM combined_sample
ORDER BY sent_date, po_id
LIMIT 5000;