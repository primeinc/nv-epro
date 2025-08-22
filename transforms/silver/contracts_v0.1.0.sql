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
    
    -- Core fields
    "Description" AS description,
    "Department" AS department,
    "Buyer" AS buyer,
    "Supplier" AS supplier,
    "Status" AS status,
    
    -- Date parsing
    "Start Date" AS start_date_raw,
    CASE
      WHEN "Start Date" IS NOT NULL AND "Start Date" != ''
      THEN strptime("Start Date", '%m/%d/%Y')::DATE
      ELSE NULL
    END AS start_date,
    
    "End Date" AS end_date_raw,
    CASE
      WHEN "End Date" IS NOT NULL AND "End Date" != ''
      THEN strptime("End Date", '%m/%d/%Y')::DATE
      ELSE NULL
    END AS end_date,
    
    -- Financial parsing
    "Contract Amount" AS contract_amount_raw,
    TRY_CAST(
      REPLACE(REPLACE("Contract Amount", '$', ''), ',', '') 
      AS DECIMAL(18,2)
    ) AS contract_amount,
    
    -- Related entities
    "Bid #" AS bid_id_raw,
    NULLIF(TRIM("Bid #"), '') AS bid_id,
    
    -- Derived columns
    CASE
      WHEN "Status" IN ('Active', 'Current') THEN TRUE
      WHEN "Status" IN ('Expired', 'Terminated', 'Cancelled') THEN FALSE
      ELSE NULL
    END AS is_active,
    
    CASE
      WHEN end_date < CURRENT_DATE THEN TRUE
      WHEN end_date >= CURRENT_DATE THEN FALSE
      ELSE NULL
    END AS is_expired,
    
    CASE
      WHEN start_date IS NOT NULL AND end_date IS NOT NULL
      THEN DATEDIFF('day', start_date, end_date)
      ELSE NULL
    END AS contract_duration_days,
    
    -- Lineage columns from Bronze
    source_system,
    source_file,
    source_row,
    ingested_at,
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
  AND (start_date IS NULL OR start_date >= DATE '2018-01-31')
  AND (start_date IS NULL OR start_date <= CURRENT_DATE + INTERVAL 365 DAY)
  AND (end_date IS NULL OR end_date >= DATE '2018-01-31')
  -- Logical validation
  AND (start_date IS NULL OR end_date IS NULL OR start_date <= end_date)
  AND (contract_amount IS NULL OR contract_amount >= 0)