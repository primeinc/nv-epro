-- Research Sample for Contracts Table
-- Target: ~800 records (50% of total 1,607)
-- Strategy: Balanced sampling across spend ranges and contract types

WITH contract_stats AS (
    SELECT 
        contract_id,
        vendor_name,
        organization,
        dollars_spent_to_date,
        fiscal_year_begin,
        is_zero_spend,
        is_multi_year,
        is_statewide_contract,
        CASE 
            WHEN dollars_spent_to_date = 0 THEN 'zero'
            WHEN dollars_spent_to_date < 10000 THEN 'small'
            WHEN dollars_spent_to_date < 100000 THEN 'medium'
            WHEN dollars_spent_to_date < 1000000 THEN 'large'
            WHEN dollars_spent_to_date < 10000000 THEN 'very_large'
            ELSE 'mega'
        END as spend_tier,
        CASE 
            WHEN fiscal_year_begin >= 2024 THEN 'recent'
            WHEN fiscal_year_begin >= 2022 THEN 'mid'
            ELSE 'historical'
        END as temporal_bucket
    FROM './data/silver/contracts/version=v0.3.0/snapshot=*/data.parquet'
),

stratified_sample AS (
    -- Zero-spend contracts (important pattern) - 200 records
    (SELECT *, 'zero_spend' as sample_reason
    FROM contract_stats
    WHERE is_zero_spend = true
    ORDER BY RANDOM()
    LIMIT 200)
    
    UNION ALL
    
    -- Small spend (<$10K) - 50 records
    (SELECT *, 'small_spend' as sample_reason
    FROM contract_stats
    WHERE spend_tier = 'small'
    ORDER BY RANDOM()
    LIMIT 50)
    
    UNION ALL
    
    -- Medium spend ($10K-$100K) - 100 records
    (SELECT *, 'medium_spend' as sample_reason
    FROM contract_stats
    WHERE spend_tier = 'medium'
    ORDER BY RANDOM()
    LIMIT 100)
    
    UNION ALL
    
    -- Large spend ($100K-$1M) - 200 records
    (SELECT *, 'large_spend' as sample_reason
    FROM contract_stats
    WHERE spend_tier = 'large'
    ORDER BY RANDOM()
    LIMIT 200)
    
    UNION ALL
    
    -- Very large spend ($1M-$10M) - 150 records
    (SELECT *, 'very_large_spend' as sample_reason
    FROM contract_stats
    WHERE spend_tier = 'very_large'
    ORDER BY RANDOM()
    LIMIT 150)
    
    UNION ALL
    
    -- Mega contracts (>$10M) - all of them
    (SELECT *, 'mega_spend' as sample_reason
    FROM contract_stats
    WHERE spend_tier = 'mega')
    
    UNION ALL
    
    -- Statewide contracts (important category)
    (SELECT *, 'statewide' as sample_reason
    FROM contract_stats
    WHERE is_statewide_contract = true
    ORDER BY RANDOM()
    LIMIT 50)
    
    UNION ALL
    
    -- Recent contracts for currency
    (SELECT *, 'recent_contracts' as sample_reason
    FROM contract_stats
    WHERE temporal_bucket = 'recent'
    ORDER BY RANDOM()
    LIMIT 50)
)

-- Final selection with deduplication
SELECT DISTINCT
    c.*,
    ss.sample_reason,
    ss.spend_tier,
    ss.temporal_bucket
FROM stratified_sample ss
JOIN './data/silver/contracts/version=v0.3.0/snapshot=*/data.parquet' c
    ON ss.contract_id = c.contract_id
ORDER BY c.dollars_spent_to_date DESC, c.fiscal_year_begin DESC
LIMIT 800;