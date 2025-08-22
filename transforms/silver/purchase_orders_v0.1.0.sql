-- Silver transformation for purchase_orders dataset v0.1.0
-- Transforms Bronze Nevada ePro PO data to normalized Silver layer
-- Author: Data Pipeline
-- Version: 0.1.0

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

transformed AS (
  SELECT
    -- Primary key
    "PO #" AS po_id,
    
    -- Revision handling
    "Revision #" AS revision_number_raw,
    TRY_CAST("Revision #" AS INTEGER) AS revision_number,
    
    -- Date parsing
    "Date Sent" AS date_sent_raw,
    CASE
      WHEN "Date Sent" IS NOT NULL AND "Date Sent" != ''
      THEN strptime("Date Sent", '%m/%d/%Y')::DATE
      ELSE NULL
    END AS date_sent,
    
    -- Delivery timeline
    "Days to Delivery" AS days_to_delivery_raw,
    TRY_CAST("Days to Delivery" AS INTEGER) AS days_to_delivery,
    
    -- Status
    "Status" AS status,
    
    -- Financial parsing
    "Total" AS total_raw,
    TRY_CAST(
      REPLACE(REPLACE("Total", '$', ''), ',', '') 
      AS DECIMAL(18,2)
    ) AS total_amount,
    
    -- Related entities with null handling
    "Bid #" AS bid_id_raw,
    NULLIF(TRIM("Bid #"), '') AS bid_id,
    
    "Contract #" AS contract_id_raw,
    NULLIF(TRIM("Contract #"), '') AS contract_id,
    
    -- Parties
    "Buyer Name" AS buyer_name,
    "Supplier" AS supplier,
    "Description" AS description,
    
    -- Derived columns
    CASE
      WHEN "Status" IN ('Complete', 'Closed') THEN TRUE
      WHEN "Status" IN ('Open', 'Pending', 'Draft') THEN FALSE
      ELSE NULL
    END AS is_complete,
    
    CASE
      WHEN TRY_CAST("Revision #" AS INTEGER) > 0 THEN TRUE
      ELSE FALSE
    END AS is_revised,
    
    -- Lineage columns from Bronze
    source_system,
    source_file,
    source_row,
    ingested_at,
    row_hash AS bronze_row_hash,
    
    -- Silver metadata
    '{transform_version}' AS transform_version,
    'purchase_orders_v0.1.0.sql' AS transform_sql,
    CURRENT_TIMESTAMP AS transformed_at,
    '{snapshot_id}' AS snapshot_id
    
  FROM bronze_data
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND po_id IS NOT NULL
  AND po_id != ''
  -- Date validation
  AND (date_sent IS NULL OR date_sent >= DATE '2018-01-31')
  AND (date_sent IS NULL OR date_sent <= CURRENT_DATE + INTERVAL 30 DAY)
  -- Business logic validation
  AND (total_amount IS NULL OR total_amount >= 0)
  AND (days_to_delivery IS NULL OR days_to_delivery >= 0)