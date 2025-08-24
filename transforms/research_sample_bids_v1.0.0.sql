-- Research Sample for Bids Table
-- Target: ~1000 records (37% of total 2,674)
-- Strategy: Include all high-value patterns since dataset is small

WITH org_counts AS (
    SELECT 
        organization,
        COUNT(*) as org_bid_count
    FROM './data/silver/bids/version=v0.3.0/snapshot=*/data.parquet'
    GROUP BY organization
),

bid_stats AS (
    SELECT 
        b.bid_solicitation_id,
        b.fiscal_year,
        b.organization,
        b.status_category,
        b.has_awarded_vendor,
        b.has_contract,
        b.awarded_vendor_count,
        CASE 
            WHEN b.fiscal_year >= 2024 THEN 'recent'
            WHEN b.fiscal_year >= 2021 THEN 'mid'
            ELSE 'historical'
        END as temporal_bucket,
        -- Rank organizations by bid count
        DENSE_RANK() OVER (ORDER BY oc.org_bid_count DESC) as org_rank
    FROM './data/silver/bids/version=v0.3.0/snapshot=*/data.parquet' b
    JOIN org_counts oc ON b.organization = oc.organization
),

stratified_sample AS (
    -- Recent bids (2024+) - 40% allocation
    (SELECT *, 'recent_period' as sample_reason 
    FROM bid_stats 
    WHERE temporal_bucket = 'recent'
    ORDER BY RANDOM() 
    LIMIT 400)
    
    UNION ALL
    
    -- Mid-period bids (2021-2023) - 35% allocation
    (SELECT *, 'mid_period' as sample_reason
    FROM bid_stats
    WHERE temporal_bucket = 'mid'
    ORDER BY RANDOM()
    LIMIT 350)
    
    UNION ALL
    
    -- Historical bids (pre-2021) - 15% allocation
    (SELECT *, 'historical_period' as sample_reason
    FROM bid_stats
    WHERE temporal_bucket = 'historical'
    ORDER BY RANDOM()
    LIMIT 150)
    
    UNION ALL
    
    -- Top organizations (ensure representation)
    (SELECT *, 'top_organization' as sample_reason
    FROM bid_stats
    WHERE org_rank <= 5
    ORDER BY RANDOM()
    LIMIT 50)
    
    UNION ALL
    
    -- Bids with contracts (important for linkage)
    (SELECT *, 'has_contract' as sample_reason
    FROM bid_stats
    WHERE has_contract = true
    ORDER BY RANDOM()
    LIMIT 50)
)

-- Final selection with deduplication
SELECT DISTINCT
    b.*,
    ss.sample_reason,
    ss.temporal_bucket,
    CASE 
        WHEN ss.org_rank <= 5 THEN 'top5_org'
        WHEN ss.org_rank <= 10 THEN 'top10_org'
        ELSE 'other_org'
    END as org_tier
FROM stratified_sample ss
JOIN './data/silver/bids/version=v0.3.0/snapshot=*/data.parquet' b
    ON ss.bid_solicitation_id = b.bid_solicitation_id
ORDER BY b.fiscal_year DESC, b.bid_opening_date DESC
LIMIT 1000;