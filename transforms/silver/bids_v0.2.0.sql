-- Silver transformation for bids dataset v0.2.0
-- Uses EXACT data types from profiling
-- Implements deduplication and bid analysis
-- Generated from exact-data-types.json

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

-- Deduplicate by taking the latest version of each bid
latest_per_bid AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY "Bid Solicitation #" 
      ORDER BY ingested_at DESC, source_row DESC
    ) AS rn
  FROM bronze_data
),

deduplicated AS (
  SELECT * FROM latest_per_bid WHERE rn = 1
),

transformed AS (
  SELECT
    -- Primary identifier (33.33% unique - multiple bids per solicitation expected)
    CAST("Bid Solicitation #" AS VARCHAR(23)) AS bid_solicitation_id,
    
    -- Related identifiers
    CAST("Contract #" AS VARCHAR(64)) AS contract_id,
    CAST("Alternate Id" AS VARCHAR(66)) AS alternate_id,
    
    -- Bid details with exact types
    CAST("Description" AS VARCHAR(132)) AS description,
    CAST("Organization Name" AS VARCHAR(63)) AS organization,
    CAST("Buyer" AS VARCHAR(29)) AS buyer_name,
    
    -- Bid opening timestamp (includes time)
    CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS TIMESTAMP) AS bid_opening_timestamp,
    
    -- Extract date and time components for analysis
    CAST(CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS DATE) AS DATE) AS bid_opening_date,
    CAST(EXTRACT(HOUR FROM strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) AS TINYINT) AS bid_opening_hour,
    CAST(EXTRACT(MINUTE FROM strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) AS TINYINT) AS bid_opening_minute,
    
    -- Day of week analysis (0=Sunday, 6=Saturday)
    CAST(DAYOFWEEK(CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS DATE)) AS TINYINT) AS bid_opening_dow,
    CAST(
      CASE DAYOFWEEK(CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS DATE))
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
      END AS VARCHAR(10)
    ) AS bid_opening_day_name,
    
    -- Business hours flag (M-F, 8AM-5PM)
    CAST(
      DAYOFWEEK(CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS DATE)) BETWEEN 1 AND 5
      AND EXTRACT(HOUR FROM strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) BETWEEN 8 AND 16
      AS BOOLEAN
    ) AS is_business_hours,
    
    -- Status with exact values from CHECK constraint
    CAST("Status" AS VARCHAR(14)) AS status_code,
    CAST(
      CASE "Status"
        WHEN 'Approved' THEN 'Approved'
        WHEN 'Bid to PO' THEN 'Converted to PO'
        WHEN 'Closed' THEN 'Closed'
        WHEN 'Evaluated' THEN 'Under Evaluation'
        WHEN 'Opened' THEN 'Open for Bidding'
        WHEN 'Sent' THEN 'Sent to Vendors'
        ELSE "Status"
      END AS VARCHAR(20)
    ) AS status_name,
    
    -- Status grouping
    CAST(
      CASE
        WHEN "Status" IN ('Approved', 'Bid to PO', 'Closed') THEN 'Complete'
        WHEN "Status" IN ('Evaluated') THEN 'In Progress'
        WHEN "Status" IN ('Opened', 'Sent') THEN 'Active'
        ELSE 'Unknown'
      END AS VARCHAR(15)
    ) AS status_category,
    
    -- Bid holder list (single value: '/bso/external/bidAckList.sdo')
    CAST("Bid Holder List" AS VARCHAR(38)) AS bid_holder_list,
    CAST("Bid Holder List" IS NOT NULL AS BOOLEAN) AS has_bid_holder_list,
    
    -- Awarded vendors (can be multiple, comma-separated)
    CAST("Awarded Vendor(s)" AS TEXT) AS awarded_vendors,
    CAST("Awarded Vendor(s)" IS NOT NULL AND LENGTH("Awarded Vendor(s)") > 0 AS BOOLEAN) AS has_awarded_vendor,
    
    -- Count awarded vendors (approximate based on commas)
    CAST(
      CASE 
        WHEN "Awarded Vendor(s)" IS NULL OR LENGTH("Awarded Vendor(s)") = 0 THEN 0
        ELSE ARRAY_LENGTH(STRING_SPLIT("Awarded Vendor(s)", ','), 1)
      END AS INTEGER
    ) AS awarded_vendor_count,
    
    -- Bid temporal analysis
    CAST(
      CASE
        WHEN strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') > CURRENT_TIMESTAMP THEN 'Future'
        WHEN "Status" IN ('Approved', 'Bid to PO', 'Closed') THEN 'Complete'
        WHEN "Status" IN ('Evaluated') THEN 'In Progress'
        WHEN "Status" IN ('Opened', 'Sent') THEN 'Active'
        ELSE 'Unknown'
      END AS VARCHAR(15)
    ) AS bid_status,
    
    -- Days since/until opening
    CAST(
      DATE_DIFF('day', 
        CAST(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S') AS DATE),
        CURRENT_DATE
      ) AS INTEGER
    ) AS days_since_opening,
    
    -- Fiscal year (Nevada FY starts July 1)
    CAST(
      CASE
        WHEN MONTH(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) >= 7
        THEN YEAR(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) + 1
        ELSE YEAR(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S'))
      END AS SMALLINT
    ) AS fiscal_year,
    
    CAST(
      CASE
        WHEN MONTH(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) IN (7,8,9) THEN 1
        WHEN MONTH(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) IN (10,11,12) THEN 2
        WHEN MONTH(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) IN (1,2,3) THEN 3
        WHEN MONTH(strptime("Bid Opening Date", '%m/%d/%Y %H:%M:%S')) IN (4,5,6) THEN 4
      END AS TINYINT
    ) AS fiscal_quarter,
    
    -- Analytical flags
    CAST("Contract #" IS NOT NULL AND LENGTH("Contract #") > 0 AS BOOLEAN) AS has_contract,
    CAST("Organization Name" = 'Statewide Contracts' AS BOOLEAN) AS is_statewide,
    CAST("Status" = 'Bid to PO' AS BOOLEAN) AS is_converted_to_po,
    
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
WHERE 1=1
  -- Data quality filters
  AND bid_solicitation_id IS NOT NULL
  AND bid_opening_timestamp IS NOT NULL
  AND bid_opening_timestamp >= TIMESTAMP '2018-01-31 00:00:00'  -- Nevada ePro start
  AND bid_opening_timestamp <= CURRENT_TIMESTAMP + INTERVAL 365 DAY  -- Max 1 year future
  -- Remove test data
  AND bid_solicitation_id NOT ILIKE '%TEST%'
  AND description NOT ILIKE '%TEST%'
  AND description NOT ILIKE '%DEMO%'
ORDER BY bid_opening_timestamp DESC, bid_solicitation_id