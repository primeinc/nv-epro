-- Silver transformation for purchase_orders dataset v0.1.0
-- Transforms Bronze Nevada ePro PO data to normalized Silver layer
-- Author: Data Pipeline
-- Version: 0.1.0

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

-- Deduplicate by taking the latest version of each PO
-- (based on ingested_at timestamp)
latest_per_po AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY "PO #" 
      ORDER BY ingested_at DESC, source_row DESC
    ) AS rn
  FROM bronze_data
),

deduplicated AS (
  SELECT * FROM latest_per_po WHERE rn = 1
),

transformed AS (
  SELECT
    -- Primary key (clean)
    TRIM("PO #") AS po_id,
    
    -- Parse revision number from PO ID if present (e.g., "99SWC-NV21-8617:1")
    CASE 
      WHEN POSITION(':' IN "PO #") > 0 
      THEN TRY_CAST(SPLIT_PART("PO #", ':', 2) AS INTEGER)
      ELSE 0
    END AS revision_number,
    
    -- Base PO ID without revision
    CASE 
      WHEN POSITION(':' IN "PO #") > 0 
      THEN SPLIT_PART("PO #", ':', 1)
      ELSE "PO #"
    END AS base_po_id,
    
    -- Date parsing
    "Sent Date" AS sent_date_raw,
    strptime("Sent Date", '%m/%d/%Y')::DATE AS sent_date,
    
    -- Decode status codes to human-readable
    "Status" AS status_code,
    CASE "Status"
      WHEN '3PS - Sent' THEN 'Sent'
      WHEN '3PCR - Complete Receipt' THEN 'Complete'
      WHEN '3PCO - Closed' THEN 'Closed'
      WHEN '3PPR - Partial Receipt' THEN 'Partial Receipt'
      WHEN '3PD - Draft' THEN 'Draft'
      WHEN '3PC - Cancelled' THEN 'Cancelled'
      ELSE SPLIT_PART("Status", ' - ', 2)  -- Extract after dash if pattern matches
    END AS status,
    
    -- Status category
    CASE 
      WHEN "Status" IN ('3PCO - Closed', '3PCR - Complete Receipt') THEN 'Complete'
      WHEN "Status" IN ('3PS - Sent', '3PPR - Partial Receipt') THEN 'Active'
      WHEN "Status" IN ('3PD - Draft') THEN 'Pending'
      WHEN "Status" IN ('3PC - Cancelled') THEN 'Cancelled'
      ELSE 'Unknown'
    END AS status_category,
    
    -- Core fields (cleaned)
    TRIM("Description") AS description,
    TRIM("Vendor") AS vendor,
    TRIM("Organization") AS organization,
    TRIM("Department") AS department,
    TRIM("Buyer") AS buyer,
    
    -- Financial parsing
    "Total" AS total_raw,
    TRY_CAST(
      REPLACE(REPLACE("Total", '$', ''), ',', '') 
      AS DECIMAL(18,2)
    ) AS total_amount,
    
    -- Fiscal year calculation (July 1 - June 30)
    CASE
      WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) >= 7
      THEN YEAR(strptime("Sent Date", '%m/%d/%Y')) + 1
      ELSE YEAR(strptime("Sent Date", '%m/%d/%Y'))
    END AS fiscal_year,
    
    -- Quarter calculation
    QUARTER(strptime("Sent Date", '%m/%d/%Y')) AS calendar_quarter,
    CASE
      WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (7,8,9) THEN 1
      WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (10,11,12) THEN 2
      WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (1,2,3) THEN 3
      WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (4,5,6) THEN 4
    END AS fiscal_quarter,
    
    -- Year/Month for partitioning
    YEAR(strptime("Sent Date", '%m/%d/%Y')) AS sent_year,
    MONTH(strptime("Sent Date", '%m/%d/%Y')) AS sent_month,
    
    -- Data quality flags
    CASE
      WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(18,2)) = 0 
        OR "Total" = '$.00' THEN TRUE
      ELSE FALSE
    END AS is_zero_dollar,
    
    CASE
      WHEN "Organization" = 'Statewide Contracts' THEN TRUE
      ELSE FALSE
    END AS is_statewide_contract,
    
    -- Temporal tracking
    ingested_at AS first_seen_at,
    ingested_at AS last_seen_at,
    1 AS version_count,
    
    -- Lineage
    source_system,
    source_file,
    source_file_hash,
    row_hash AS bronze_row_hash,
    
    -- Silver metadata
    '{transform_version}' AS transform_version,
    CURRENT_TIMESTAMP AS transformed_at,
    '{snapshot_id}' AS snapshot_id
    
  FROM deduplicated
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND po_id IS NOT NULL
  AND po_id != ''
  -- Date validation (Nevada ePro starts Jan 31, 2018)
  AND sent_date >= DATE '2018-01-31'
  AND sent_date <= CURRENT_DATE + INTERVAL 30 DAY
  -- Business logic validation
  AND (total_amount IS NULL OR total_amount >= 0)
  -- Remove obvious test data
  AND po_id NOT LIKE '%TEST%'
  AND po_id NOT LIKE '%DEMO%'
  AND po_id NOT LIKE '%TEMP%'