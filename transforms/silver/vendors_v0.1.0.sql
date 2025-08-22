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
    
    -- Location fields
    "Address" AS address,
    "City" AS city,
    "State" AS state,
    "Postal Code" AS postal_code,
    
    -- Contact fields
    "Contact Name" AS contact_name,
    "Phone" AS phone,
    
    -- Derived columns
    CASE
      WHEN "State" IN ('NV', 'Nevada') THEN TRUE
      ELSE FALSE
    END AS is_nevada_vendor,
    
    -- Clean phone number for analysis
    REGEXP_REPLACE("Phone", '[^0-9]', '') AS phone_digits_only,
    
    -- Extract ZIP5 from postal code
    CASE
      WHEN "Postal Code" IS NOT NULL
      THEN SUBSTRING(REGEXP_REPLACE("Postal Code", '[^0-9]', ''), 1, 5)
      ELSE NULL
    END AS zip5,
    
    -- Lineage columns from Bronze
    source_system,
    source_file,
    source_file_hash,
    source_row,
    ingested_at,
    bronze_run_id,
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
  -- Remove test data
  AND vendor_name NOT LIKE '%TEST%'
  AND vendor_name NOT LIKE '%DEMO%'
  AND vendor_name NOT LIKE '%DO NOT USE%'