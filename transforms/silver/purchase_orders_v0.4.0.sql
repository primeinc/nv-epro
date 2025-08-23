-- Silver transformation for purchase_orders dataset v0.4.0
-- Uses EXACT data types from profiling
-- Implements deduplication, normalization, and business logic
-- Generated from exact-data-types.json

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

-- Deduplicate by taking the latest version of each PO
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
    -- Primary identifier (99.83% unique, almost a natural key)
    CAST("PO #" AS VARCHAR(43)) AS po_id,
    
    -- Parse base PO and revision
    CAST(
      CASE 
        WHEN POSITION(':' IN "PO #") > 0 THEN SPLIT_PART("PO #", ':', 1)
        WHEN POSITION('.' IN "PO #") > 0 THEN SPLIT_PART("PO #", '.', 1)
        ELSE "PO #"
      END AS VARCHAR(43)
    ) AS base_po_id,
    
    CAST(
      CASE 
        WHEN POSITION(':' IN "PO #") > 0 THEN TRY_CAST(SPLIT_PART("PO #", ':', 2) AS SMALLINT)
        WHEN POSITION('.' IN "PO #") > 0 THEN TRY_CAST(SPLIT_PART("PO #", '.', 2) AS SMALLINT)
        ELSE 0
      END AS SMALLINT
    ) AS revision_number,
    
    -- Core fields with exact types
    CAST(strptime("Sent Date", '%m/%d/%Y') AS DATE) AS sent_date,
    CAST(TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) AS DECIMAL(15,2)) AS total_amount,
    CAST("Description" AS VARCHAR(132)) AS description,
    CAST("Vendor" AS VARCHAR(108)) AS vendor_name,
    CAST("Organization" AS VARCHAR(63)) AS organization,
    CAST("Department" AS VARCHAR(65)) AS department,
    CAST("Buyer" AS VARCHAR(40)) AS buyer_name,
    
    -- Status with decoded values
    CAST("Status" AS VARCHAR(28)) AS status_code,
    CAST(
      CASE "Status"
        WHEN '3PS - Sent' THEN 'Sent'
        WHEN '3PCR - Complete Receipt' THEN 'Complete Receipt'
        WHEN '3PCO - Closed' THEN 'Closed'
        WHEN '3PPR - Partial Receipt' THEN 'Partial Receipt'
        ELSE SPLIT_PART("Status", ' - ', 2)
      END AS VARCHAR(20)
    ) AS status_name,
    
    CAST(
      CASE 
        WHEN "Status" IN ('3PCO - Closed', '3PCR - Complete Receipt') THEN 'Complete'
        WHEN "Status" IN ('3PS - Sent', '3PPR - Partial Receipt') THEN 'Active'
        ELSE 'Unknown'
      END AS VARCHAR(10)
    ) AS status_category,
    
    -- Fiscal year calculations (Nevada FY starts July 1)
    CAST(
      CASE
        WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) >= 7
        THEN YEAR(strptime("Sent Date", '%m/%d/%Y')) + 1
        ELSE YEAR(strptime("Sent Date", '%m/%d/%Y'))
      END AS SMALLINT
    ) AS fiscal_year,
    
    CAST(
      CASE
        WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (7,8,9) THEN 1
        WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (10,11,12) THEN 2
        WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (1,2,3) THEN 3
        WHEN MONTH(strptime("Sent Date", '%m/%d/%Y')) IN (4,5,6) THEN 4
      END AS TINYINT
    ) AS fiscal_quarter,
    
    CAST(YEAR(strptime("Sent Date", '%m/%d/%Y')) AS SMALLINT) AS calendar_year,
    CAST(MONTH(strptime("Sent Date", '%m/%d/%Y')) AS TINYINT) AS calendar_month,
    CAST(QUARTER(strptime("Sent Date", '%m/%d/%Y')) AS TINYINT) AS calendar_quarter,
    
    -- Analytical flags
    CAST(TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) = 0 AS BOOLEAN) AS is_zero_dollar,
    CAST("Organization" = 'Statewide Contracts' AS BOOLEAN) AS is_statewide_contract,
    CAST("Description" LIKE 'G2B%' AS BOOLEAN) AS is_g2b_purchase,
    
    -- Vendor categorization
    CAST(
      CASE
        WHEN "Vendor" ILIKE '%amazon%' THEN 'Amazon'
        WHEN "Vendor" ILIKE '%staples%' THEN 'Staples'
        WHEN "Vendor" ILIKE '%dell%' THEN 'Dell'
        WHEN "Vendor" ILIKE '%grainger%' THEN 'Grainger'
        WHEN "Vendor" ILIKE '%cdw%' THEN 'CDW'
        WHEN "Vendor" ILIKE '%office depot%' OR "Vendor" ILIKE '%ODP%' THEN 'Office Depot'
        WHEN "Vendor" ILIKE '%home depot%' THEN 'Home Depot'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS vendor_category,
    
    -- Amount categorization
    CAST(
      CASE
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) = 0 THEN 'Zero'
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) < 1000 THEN 'Micro (<$1K)'
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) < 10000 THEN 'Small ($1K-$10K)'
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) < 100000 THEN 'Medium ($10K-$100K)'
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) < 1000000 THEN 'Large ($100K-$1M)'
        ELSE 'Mega (>$1M)'
      END AS VARCHAR(20)
    ) AS amount_category,
    
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
WHERE po_id IS NOT NULL  -- Only require non-null primary key
ORDER BY sent_date DESC, po_id