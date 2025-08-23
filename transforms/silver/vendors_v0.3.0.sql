-- Silver transformation for vendors dataset v0.3.0
-- Uses EXACT data types from profiling
-- Implements deduplication and vendor enrichment
-- Generated from exact-data-types.json

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

-- Deduplicate by taking the latest version of each vendor
latest_per_vendor AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY "Vendor ID" 
      ORDER BY ingested_at DESC, source_row DESC
    ) AS rn
  FROM bronze_data
),

deduplicated AS (
  SELECT * FROM latest_per_vendor WHERE rn = 1
),

transformed AS (
  SELECT
    -- Primary key (100% unique)
    CAST("Vendor ID" AS VARCHAR(18)) AS vendor_id,
    
    -- Vendor details with exact types
    CAST("Vendor Name" AS VARCHAR(108)) AS vendor_name,
    
    -- Clean and standardize vendor name for matching
    CAST(UPPER(TRIM(REGEXP_REPLACE("Vendor Name", '[^A-Za-z0-9 ]', '', 'g'))) AS VARCHAR(108)) AS vendor_name_clean,
    
    -- Address fields (all currently null but keeping for future data)
    CAST("Address" AS VARCHAR(100)) AS address,
    CAST("City" AS VARCHAR(50)) AS city,
    CAST("State" AS VARCHAR(2)) AS state,
    CAST("Postal Code" AS VARCHAR(10)) AS postal_code,
    
    -- Contact information
    CAST("Contact Name" AS VARCHAR(64)) AS contact_name,
    CAST("Phone" AS VARCHAR(24)) AS phone,
    
    -- Clean phone for analysis (digits only)
    CAST(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g') AS VARCHAR(20)) AS phone_digits,
    
    -- Phone type classification
    CAST(
      CASE
        WHEN LENGTH(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g')) = 10 THEN 'Standard'
        WHEN LENGTH(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g')) = 11 AND REGEXP_REPLACE("Phone", '[^0-9]', '', 'g') LIKE '1%' THEN 'Standard with 1'
        WHEN LENGTH(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g')) > 10 AND "Phone" ILIKE '%ext%' THEN 'With Extension'
        WHEN LENGTH(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g')) < 10 THEN 'Incomplete'
        ELSE 'Non-standard'
      END AS VARCHAR(20)
    ) AS phone_type,
    
    -- Vendor categorization based on name patterns
    CAST(
      CASE
        WHEN "Vendor Name" ILIKE '%LLC%' OR "Vendor Name" ILIKE '%, LLC%' THEN 'LLC'
        WHEN "Vendor Name" ILIKE '%INC%' OR "Vendor Name" ILIKE '%, INC%' THEN 'Corporation'
        WHEN "Vendor Name" ILIKE '%CORP%' THEN 'Corporation'
        WHEN "Vendor Name" ILIKE '%COMPANY%' OR "Vendor Name" ILIKE '% CO %' OR "Vendor Name" ILIKE '% CO.' THEN 'Company'
        WHEN "Vendor Name" ILIKE '%PARTNERSHIP%' THEN 'Partnership'
        WHEN "Vendor Name" ILIKE '%FOUNDATION%' THEN 'Foundation'
        WHEN "Vendor Name" ILIKE '%UNIVERSITY%' OR "Vendor Name" ILIKE '%COLLEGE%' THEN 'Educational'
        WHEN "Vendor Name" ILIKE '%GOVERNMENT%' OR "Vendor Name" ILIKE '%STATE OF%' OR "Vendor Name" ILIKE '%CITY OF%' THEN 'Government'
        WHEN "Vendor Name" ILIKE '%DBA%' THEN 'DBA'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS vendor_type,
    
    -- Major vendor identification
    CAST(
      CASE
        WHEN "Vendor Name" ILIKE '%amazon%' THEN 'Amazon'
        WHEN "Vendor Name" ILIKE '%staples%' THEN 'Staples'
        WHEN "Vendor Name" ILIKE '%dell%' THEN 'Dell'
        WHEN "Vendor Name" ILIKE '%grainger%' THEN 'Grainger'
        WHEN "Vendor Name" ILIKE '%cdw%' THEN 'CDW'
        WHEN "Vendor Name" ILIKE '%office depot%' OR "Vendor Name" ILIKE '%ODP%' THEN 'Office Depot'
        WHEN "Vendor Name" ILIKE '%home depot%' THEN 'Home Depot'
        WHEN "Vendor Name" ILIKE '%microsoft%' THEN 'Microsoft'
        WHEN "Vendor Name" ILIKE '%oracle%' THEN 'Oracle'
        WHEN "Vendor Name" ILIKE '%adobe%' THEN 'Adobe'
        WHEN "Vendor Name" ILIKE '%cisco%' THEN 'Cisco'
        WHEN "Vendor Name" ILIKE '%ibm%' THEN 'IBM'
        WHEN "Vendor Name" ILIKE '%hp %' OR "Vendor Name" ILIKE '%hewlett%' THEN 'HP'
        WHEN "Vendor Name" ILIKE '%xerox%' THEN 'Xerox'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS vendor_category,
    
    -- Industry classification based on name patterns
    CAST(
      CASE
        WHEN "Vendor Name" ILIKE '%software%' OR "Vendor Name" ILIKE '%technology%' OR "Vendor Name" ILIKE '%computer%' THEN 'Technology'
        WHEN "Vendor Name" ILIKE '%medical%' OR "Vendor Name" ILIKE '%health%' OR "Vendor Name" ILIKE '%pharma%' THEN 'Healthcare'
        WHEN "Vendor Name" ILIKE '%construction%' OR "Vendor Name" ILIKE '%building%' OR "Vendor Name" ILIKE '%contractor%' THEN 'Construction'
        WHEN "Vendor Name" ILIKE '%consulting%' OR "Vendor Name" ILIKE '%advisory%' THEN 'Consulting'
        WHEN "Vendor Name" ILIKE '%transport%' OR "Vendor Name" ILIKE '%logistics%' OR "Vendor Name" ILIKE '%freight%' THEN 'Transportation'
        WHEN "Vendor Name" ILIKE '%electric%' OR "Vendor Name" ILIKE '%energy%' OR "Vendor Name" ILIKE '%power%' THEN 'Energy'
        WHEN "Vendor Name" ILIKE '%food%' OR "Vendor Name" ILIKE '%catering%' OR "Vendor Name" ILIKE '%restaurant%' THEN 'Food Service'
        WHEN "Vendor Name" ILIKE '%office%' OR "Vendor Name" ILIKE '%supplies%' OR "Vendor Name" ILIKE '%stationery%' THEN 'Office Supplies'
        ELSE 'Other'
      END AS VARCHAR(30)
    ) AS industry_category,
    
    -- Data quality flags
    CAST("Address" IS NULL AND "City" IS NULL AND "State" IS NULL AS BOOLEAN) AS is_missing_address,
    CAST(LENGTH(REGEXP_REPLACE("Phone", '[^0-9]', '', 'g')) != 10 AS BOOLEAN) AS is_non_standard_phone,
    
    -- Lineage tracking
    CAST(source_file_hash AS VARCHAR(64)) AS source_hash,
    CAST(row_hash AS VARCHAR(64)) AS row_hash,
    CAST(ingested_at AS TIMESTAMP) AS bronze_ingested_at,
    
    -- Silver metadata
    CAST('{transform_version}' AS VARCHAR(10)) AS transform_version,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at,
    CAST('{snapshot_id}' AS VARCHAR(50)) AS snapshot_id
    
  FROM deduplicated
)

SELECT * FROM transformed
WHERE vendor_id IS NOT NULL  -- Only require non-null primary key
ORDER BY vendor_name