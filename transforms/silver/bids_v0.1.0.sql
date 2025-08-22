-- Silver transformation for bids dataset v0.1.0
-- Transforms Bronze Nevada ePro bid data to normalized Silver layer
-- Author: Data Pipeline
-- Version: 0.1.0

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

transformed AS (
  SELECT
    -- Primary key
    "Bid Solicitation #" AS bid_id,
    
    -- Core fields (preserved)
    "Organization Name" AS organization,
    "Buyer" AS buyer,
    "Description" AS description,
    "Status" AS status,
    
    -- Contract ID with null handling
    "Contract #" AS contract_id_raw,
    NULLIF(TRIM("Contract #"), '') AS contract_id,
    
    -- Date parsing with validation
    "Bid Opening Date" AS bid_opening_date_raw,
    CASE 
      WHEN "Bid Opening Date" IS NOT NULL AND "Bid Opening Date" != ''
      THEN strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')::TIMESTAMP
      ELSE NULL
    END AS bid_opening_date,
    
    -- Bid holder list feature engineering
    "Bid Holder List" AS bid_holder_list_raw,
    ("Bid Holder List" = '/bso/external/bidAckList.sdo')::BOOLEAN AS has_holder_list,
    
    -- Reconstruct holder list URL when applicable
    CASE 
      WHEN "Bid Holder List" = '/bso/external/bidAckList.sdo'
      THEN 'https://nevadaepro.com/bso/external/bidAckList.sdo?bidId=' || "Bid Solicitation #"
      ELSE NULL
    END AS bid_holder_list_url,
    
    -- Parse URL components for analytics
    CASE 
      WHEN "Bid Holder List" = '/bso/external/bidAckList.sdo'
      THEN 'nevadaepro.com'
      ELSE NULL
    END AS bid_holder_list_url_host,
    
    CASE 
      WHEN "Bid Holder List" = '/bso/external/bidAckList.sdo'
      THEN '/bso/external/bidAckList.sdo'
      ELSE NULL
    END AS bid_holder_list_url_path,
    
    -- Awarded vendors with null handling
    "Awarded Vendor(s)" AS awarded_vendors_raw,
    NULLIF(TRIM("Awarded Vendor(s)"), '') AS awarded_vendors,
    
    -- Alternate ID with null handling
    "Alternate Id" AS alternate_id_raw,
    NULLIF(TRIM("Alternate Id"), ''),
    
    -- Derived columns
    CASE
      WHEN "Status" = 'Awarded' THEN TRUE
      WHEN "Status" IN ('Sent', 'Open') THEN FALSE
      ELSE NULL
    END AS is_awarded,
    
    -- Lineage columns from Bronze
    source_system,
    source_file,
    source_row,
    ingested_at,
    row_hash AS bronze_row_hash,
    
    -- Silver metadata
    '{transform_version}' AS transform_version,
    'bids_v0.1.0.sql' AS transform_sql,
    CURRENT_TIMESTAMP AS transformed_at,
    '{snapshot_id}' AS snapshot_id
    
  FROM bronze_data
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND bid_id IS NOT NULL
  AND bid_id != ''
  -- Date range validation
  AND (bid_opening_date IS NULL OR bid_opening_date >= DATE '2018-01-31')
  AND (bid_opening_date IS NULL OR bid_opening_date <= CURRENT_DATE + INTERVAL 365 DAY)