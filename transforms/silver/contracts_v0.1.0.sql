-- Silver transformation for contracts dataset v0.1.0
-- Transforms Bronze Nevada ePro contract data to normalized Silver layer
-- Author: Data Pipeline
-- Version: 0.1.0

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

transformed AS (
  SELECT
    -- Primary key
    "Contract #" AS contract_id,
    
    -- Related entities
    "Bid Solicitation #" AS bid_solicitation_id,
    
    -- Core fields
    "Description" AS description,
    "Vendor" AS vendor,
    "Type Code" AS type_code,
    "Organization" AS organization,
    "Status" AS status,
    
    -- Financial parsing
    "Dollars Spent to Date" AS dollars_spent_raw,
    TRY_CAST(
      REPLACE(REPLACE("Dollars Spent to Date", '$', ''), ',', '') 
      AS DECIMAL(18,2)
    ) AS dollars_spent,
    
    -- Date parsing
    "Begin Date" AS begin_date_raw,
    CASE
      WHEN "Begin Date" IS NOT NULL AND "Begin Date" != ''
      THEN strptime("Begin Date", '%m/%d/%Y')::DATE
      ELSE NULL
    END AS begin_date,
    
    "End Date" AS end_date_raw,
    CASE
      WHEN "End Date" IS NOT NULL AND "End Date" != ''
      THEN strptime("End Date", '%m/%d/%Y')::DATE
      ELSE NULL
    END AS end_date,
    
    -- Derived columns
    CASE
      WHEN "Status" IN ('Active', 'Current', 'Open') THEN TRUE
      WHEN "Status" IN ('Expired', 'Terminated', 'Cancelled', 'Closed') THEN FALSE
      ELSE NULL
    END AS is_active,
    
    -- Contract duration in days
    CASE
      WHEN strptime("Begin Date", '%m/%d/%Y') IS NOT NULL 
       AND strptime("End Date", '%m/%d/%Y') IS NOT NULL
      THEN DATE_DIFF('day', 
        strptime("Begin Date", '%m/%d/%Y')::DATE,
        strptime("End Date", '%m/%d/%Y')::DATE
      )
      ELSE NULL
    END AS contract_duration_days,
    
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
    'contracts_v0.1.0.sql' AS transform_sql,
    CURRENT_TIMESTAMP AS transformed_at,
    '{snapshot_id}' AS snapshot_id
    
  FROM bronze_data
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND contract_id IS NOT NULL
  AND contract_id != ''
  -- Date validation
  AND (begin_date IS NULL OR begin_date >= DATE '2000-01-01')
  AND (begin_date IS NULL OR begin_date <= CURRENT_DATE + INTERVAL 365 DAY)
  AND (end_date IS NULL OR end_date >= DATE '2000-01-01')
  -- Business logic validation
  AND (dollars_spent IS NULL OR dollars_spent >= 0)
  -- Remove test data
  AND contract_id NOT LIKE '%TEST%'
  AND contract_id NOT LIKE '%DEMO%'