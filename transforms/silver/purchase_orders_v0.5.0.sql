-- Silver transformation for purchase_orders dataset v0.5.0
-- Uses EXACT data types from profiling
-- NO DEDUPLICATION - Bronze layer already handles this correctly
-- Generated from exact-data-types.json

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

transformed AS (
  SELECT
    -- Primary identifier
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
    CAST("Organization" AS VARCHAR(90)) AS organization,
    CAST("Department" AS VARCHAR(105)) AS department,
    CAST("Buyer" AS VARCHAR(31)) AS buyer_name,
    CAST("Status" AS VARCHAR(24)) AS status,
    
    -- Derived fields
    CAST(EXTRACT(YEAR FROM strptime("Sent Date", '%m/%d/%Y')) AS SMALLINT) AS fiscal_year,
    CAST(EXTRACT(QUARTER FROM strptime("Sent Date", '%m/%d/%Y')) AS TINYINT) AS fiscal_quarter,
    CAST(EXTRACT(MONTH FROM strptime("Sent Date", '%m/%d/%Y')) AS TINYINT) AS fiscal_month,
    
    -- Extract organization code from PO (e.g., "99SWC" from "99SWC-NV23-12345")
    CAST(
      CASE 
        WHEN POSITION('-' IN "PO #") > 0 THEN SPLIT_PART("PO #", '-', 1)
        ELSE NULL
      END AS VARCHAR(10)
    ) AS org_code,
    
    -- Extract fiscal year from PO (e.g., "23" from "99SWC-NV23-12345")
    CAST(
      CASE 
        WHEN POSITION('-NV' IN "PO #") > 0 
        THEN TRY_CAST('20' || SUBSTRING(SPLIT_PART("PO #", '-NV', 2), 1, 2) AS SMALLINT)
        ELSE NULL
      END AS SMALLINT
    ) AS po_fiscal_year,
    
    -- Status categories
    CAST(
      CASE 
        WHEN "Status" LIKE '%Closed%' OR "Status" LIKE '%3PCO%' THEN 'Closed'
        WHEN "Status" LIKE '%Complete%' OR "Status" LIKE '%3PCR%' THEN 'Complete'
        WHEN "Status" LIKE '%Cancelled%' OR "Status" LIKE '%3PCA%' THEN 'Cancelled'
        WHEN "Status" LIKE '%Sent%' OR "Status" LIKE '%3PS%' THEN 'Sent'
        WHEN "Status" LIKE '%Partial%' OR "Status" LIKE '%3PPR%' THEN 'Partial'
        WHEN "Status" LIKE '%Draft%' OR "Status" LIKE '%1D%' THEN 'Draft'
        WHEN "Status" LIKE '%Hold%' OR "Status" LIKE '%3PPOH%' THEN 'Hold'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS status_category,
    
    -- Data quality indicators
    CAST(
      CASE 
        WHEN "Total" IS NULL OR "Total" = '' THEN FALSE
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(15,2)) IS NULL THEN FALSE
        ELSE TRUE
      END AS BOOLEAN
    ) AS has_valid_amount,
    
    CAST(
      CASE 
        WHEN "Sent Date" IS NULL OR "Sent Date" = '' THEN FALSE
        WHEN strptime("Sent Date", '%m/%d/%Y') IS NULL THEN FALSE
        ELSE TRUE
      END AS BOOLEAN
    ) AS has_valid_date,
    
    -- Add duplicate instance marker
    ROW_NUMBER() OVER (PARTITION BY "PO #" ORDER BY source_row) AS instance_number,
    COUNT(*) OVER (PARTITION BY "PO #") AS total_instances,
    
    -- Bronze metadata passthrough
    source_system,
    source_file,
    source_file_hash,
    source_file_bytes,
    source_row,
    ingested_at,
    bronze_run_id,
    row_hash,
    
    -- Silver metadata
    CURRENT_TIMESTAMP AS silver_processed_at,
    'v0.5.0' AS silver_version
    
  FROM bronze_data
)

SELECT * FROM transformed