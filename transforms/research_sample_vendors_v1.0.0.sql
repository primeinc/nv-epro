-- Research Sample for Vendors Table  
-- Target: ~2000 records (10% of total 19,573)
-- Strategy: Focus on vendor diversity and type representation

WITH vendor_stats AS (
    SELECT 
        vendor_id,
        vendor_name,
        vendor_name_clean,
        vendor_type,
        vendor_category,
        industry_category,
        is_non_standard_phone,
        -- Create vendor importance score based on name patterns
        CASE 
            WHEN LOWER(vendor_name) LIKE '%amazon%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%staples%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%dell%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%microsoft%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%oracle%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%ibm%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%cisco%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%adobe%' THEN 'major'
            WHEN LOWER(vendor_name) LIKE '%cdw%' THEN 'major'
            WHEN vendor_category != 'Other' THEN 'known_brand'
            ELSE 'standard'
        END as vendor_importance,
        -- Vendor type grouping
        CASE 
            WHEN vendor_type IN ('Corporation', 'LLC', 'Company') THEN 'business_entity'
            WHEN vendor_type IN ('Government', 'Educational', 'Foundation') THEN 'institutional'
            WHEN vendor_type IN ('DBA', 'Partnership') THEN 'small_business'
            ELSE 'other'
        END as vendor_group
    FROM './data/silver/vendors/version=v0.3.0/snapshot=*/data.parquet'
),

stratified_sample AS (
    -- Major vendors (all of them - they're rare and important)
    (SELECT *, 'major_vendor' as sample_reason
    FROM vendor_stats
    WHERE vendor_importance = 'major')
    
    UNION ALL
    
    -- Known brand vendors
    (SELECT *, 'known_brand' as sample_reason
    FROM vendor_stats
    WHERE vendor_importance = 'known_brand')
    
    UNION ALL
    
    -- Corporations - 600 records
    (SELECT *, 'corporation' as sample_reason
    FROM vendor_stats
    WHERE vendor_type = 'Corporation'
    ORDER BY RANDOM()
    LIMIT 600)
    
    UNION ALL
    
    -- LLCs - 500 records
    (SELECT *, 'llc' as sample_reason
    FROM vendor_stats
    WHERE vendor_type = 'LLC'
    ORDER BY RANDOM()
    LIMIT 500)
    
    UNION ALL
    
    -- Other business types - 200 records
    (SELECT *, 'other_business' as sample_reason
    FROM vendor_stats
    WHERE vendor_type IN ('Company', 'DBA', 'Partnership')
    ORDER BY RANDOM()
    LIMIT 200)
    
    UNION ALL
    
    -- Institutional vendors (all - they're rare)
    (SELECT *, 'institutional' as sample_reason
    FROM vendor_stats
    WHERE vendor_type IN ('Government', 'Educational', 'Foundation'))
    
    UNION ALL
    
    -- Random sample from "Other" category - 600 records
    (SELECT *, 'general_sample' as sample_reason
    FROM vendor_stats
    WHERE vendor_type = 'Other'
    ORDER BY RANDOM()
    LIMIT 600)
    
    UNION ALL
    
    -- Vendors with non-standard phones (data quality indicator)
    (SELECT *, 'non_standard_phone' as sample_reason
    FROM vendor_stats
    WHERE is_non_standard_phone = true
    ORDER BY RANDOM()
    LIMIT 100)
)

-- Final selection with deduplication
SELECT DISTINCT
    v.*,
    ss.sample_reason,
    ss.vendor_importance,
    ss.vendor_group
FROM stratified_sample ss
JOIN './data/silver/vendors/version=v0.3.0/snapshot=*/data.parquet' v
    ON ss.vendor_id = v.vendor_id
ORDER BY 
    CASE ss.vendor_importance
        WHEN 'major' THEN 1
        WHEN 'known_brand' THEN 2
        ELSE 3
    END,
    v.vendor_name
LIMIT 2000;