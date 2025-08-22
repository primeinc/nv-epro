-- Silver transformation for vendors dataset v0.1.0
-- Transforms Bronze Nevada ePro vendor data to normalized Silver layer
-- Author: Data Pipeline
-- Version: 0.1.0

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

transformed AS (
  SELECT
    -- Primary key
    "Vendor ID" AS vendor_id,
    
    -- Core fields
    "Vendor Name" AS vendor_name,
    "Status" AS status,
    
    -- Location fields
    "City" AS city,
    "State" AS state,
    "Zip" AS zip,
    "Country" AS country,
    
    -- Business classification
    "Business Type" AS business_type,
    "Commodity Codes" AS commodity_codes_raw,
    
    -- Parsed commodity codes (array if comma-separated)
    CASE
      WHEN "Commodity Codes" IS NOT NULL AND "Commodity Codes" != ''
      THEN string_split("Commodity Codes", ',')
      ELSE NULL
    END AS commodity_codes_array,
    
    -- Count of commodity codes
    CASE
      WHEN "Commodity Codes" IS NOT NULL AND "Commodity Codes" != ''
      THEN array_length(string_split("Commodity Codes", ','))
      ELSE 0
    END AS commodity_code_count,
    
    -- Derived columns
    CASE
      WHEN "Status" IN ('Active', 'Approved') THEN TRUE
      WHEN "Status" IN ('Inactive', 'Suspended', 'Debarred') THEN FALSE
      ELSE NULL
    END AS is_active,
    
    CASE
      WHEN "State" IN ('NV', 'Nevada') THEN TRUE
      WHEN "State" IS NOT NULL AND "State" != '' THEN FALSE
      ELSE NULL
    END AS is_nevada_vendor,
    
    CASE
      WHEN "Country" IN ('US', 'USA', 'United States', '') OR "Country" IS NULL THEN TRUE
      ELSE FALSE
    END AS is_domestic,
    
    -- Standardize state codes
    CASE
      WHEN "State" = 'Nevada' THEN 'NV'
      WHEN "State" = 'California' THEN 'CA'
      WHEN "State" = 'Arizona' THEN 'AZ'
      WHEN "State" = 'Utah' THEN 'UT'
      WHEN LENGTH("State") = 2 THEN UPPER("State")
      ELSE "State"
    END AS state_code,
    
    -- Clean zip code (first 5 digits)
    CASE
      WHEN "Zip" IS NOT NULL AND LENGTH("Zip") >= 5
      THEN SUBSTRING("Zip", 1, 5)
      ELSE "Zip"
    END AS zip5,
    
    -- Lineage columns from Bronze
    source_system,
    source_file,
    source_row,
    ingested_at,
    row_hash AS bronze_row_hash,
    
    -- Silver metadata
    '{transform_version}' AS transform_version,
    'vendors_v0.1.0.sql' AS transform_sql,
    CURRENT_TIMESTAMP AS transformed_at,
    '{snapshot_id}' AS snapshot_id
    
  FROM bronze_data
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND vendor_id IS NOT NULL
  AND vendor_id != ''
  AND vendor_name IS NOT NULL
  AND vendor_name != ''
  -- Business logic validation
  AND (state IS NULL OR LENGTH(state) <= 50)
  AND (zip IS NULL OR LENGTH(zip) <= 10)