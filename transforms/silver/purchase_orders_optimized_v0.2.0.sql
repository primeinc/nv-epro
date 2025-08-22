-- Silver transformation for purchase_orders dataset v0.2.0
-- Optimized schema based on data profiling
-- Lossless transformations with proper data types
-- Author: Data Pipeline
-- Version: 0.2.0

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

-- Create dimension lookups
organization_dim AS (
  SELECT DISTINCT
    "Organization" AS organization_name,
    DENSE_RANK() OVER (ORDER BY "Organization") AS organization_id
  FROM deduplicated
),

department_dim AS (
  SELECT DISTINCT
    "Department" AS department_name,
    DENSE_RANK() OVER (ORDER BY "Department") AS department_id
  FROM deduplicated
),

status_dim AS (
  SELECT DISTINCT
    "Status" AS status_code,
    DENSE_RANK() OVER (ORDER BY "Status") AS status_id,
    CASE "Status"
      WHEN '3PS - Sent' THEN 'Sent'
      WHEN '3PCR - Complete Receipt' THEN 'Complete Receipt'
      WHEN '3PCO - Closed' THEN 'Closed'
      WHEN '3PPR - Partial Receipt' THEN 'Partial Receipt'
      ELSE SPLIT_PART("Status", ' - ', 2)
    END AS status_name,
    CASE 
      WHEN "Status" IN ('3PCO - Closed', '3PCR - Complete Receipt') THEN 'Complete'
      WHEN "Status" IN ('3PS - Sent', '3PPR - Partial Receipt') THEN 'Active'
      ELSE 'Unknown'
    END AS status_category
  FROM deduplicated
),

transformed AS (
  SELECT
    -- Primary key: VARCHAR(32) NOT NULL
    CAST(TRIM("PO #") AS VARCHAR(32)) AS po_id,
    
    -- Parse base PO and revision (handle both : and . separators)
    CAST(
      CASE 
        WHEN POSITION(':' IN "PO #") > 0 THEN SPLIT_PART("PO #", ':', 1)
        WHEN POSITION('.' IN "PO #") > 0 THEN SPLIT_PART("PO #", '.', 1)
        ELSE "PO #"
      END AS VARCHAR(32)
    ) AS base_po_id,
    
    CAST(
      CASE 
        WHEN POSITION(':' IN "PO #") > 0 THEN TRY_CAST(SPLIT_PART("PO #", ':', 2) AS SMALLINT)
        WHEN POSITION('.' IN "PO #") > 0 THEN TRY_CAST(SPLIT_PART("PO #", '.', 2) AS SMALLINT)
        ELSE 0
      END AS SMALLINT
    ) AS revision_number,
    
    -- Date: DATE NOT NULL
    CAST(strptime("Sent Date", '%m/%d/%Y') AS DATE) AS sent_date,
    
    -- Financial: DECIMAL(12,2) NOT NULL
    CAST(
      COALESCE(
        TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(12,2)),
        0.00
      ) AS DECIMAL(12,2)
    ) AS total_amount,
    
    -- Description: VARCHAR(120)
    CAST(TRIM("Description") AS VARCHAR(120)) AS description,
    
    -- Vendor: VARCHAR(100) - normalized but keep original for now
    CAST(TRIM("Vendor") AS VARCHAR(100)) AS vendor_name,
    
    -- Buyer: VARCHAR(60)
    CAST(TRIM("Buyer") AS VARCHAR(60)) AS buyer_name,
    
    -- Dimension foreign keys
    o.organization_id AS organization_id,
    d.department_id AS department_id,
    s.status_id AS status_id,
    
    -- Denormalized for convenience (can be removed if using proper star schema)
    o.organization_name AS organization,
    d.department_name AS department,
    s.status_name AS status,
    s.status_category AS status_category,
    
    -- Fiscal calculations
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
    
    -- Flags (BIT/BOOLEAN)
    CAST(
      CASE
        WHEN TRY_CAST(REPLACE(REPLACE("Total", '$', ''), ',', '') AS DECIMAL(12,2)) = 0 
        THEN TRUE ELSE FALSE
      END AS BOOLEAN
    ) AS is_zero_dollar,
    
    CAST(
      CASE
        WHEN "Organization" = 'Statewide Contracts' THEN TRUE
        ELSE FALSE
      END AS BOOLEAN
    ) AS is_statewide_contract,
    
    CAST(
      CASE
        WHEN "Description" LIKE 'G2B%' THEN TRUE
        ELSE FALSE
      END AS BOOLEAN
    ) AS is_g2b_purchase,
    
    -- Vendor classification
    CAST(
      CASE
        WHEN "Vendor" LIKE '%Amazon%' THEN 'Amazon'
        WHEN "Vendor" LIKE '%Staples%' THEN 'Staples'
        WHEN "Vendor" LIKE '%Dell%' THEN 'Dell'
        WHEN "Vendor" LIKE '%Grainger%' THEN 'Grainger'
        WHEN "Vendor" LIKE '%ODP%' OR "Vendor" LIKE '%Office Depot%' THEN 'Office Depot'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS vendor_category,
    
    -- Temporal tracking (for SCD Type 2 if needed later)
    CAST(ingested_at AS TIMESTAMP) AS first_seen_at,
    CAST(ingested_at AS TIMESTAMP) AS last_seen_at,
    CAST(1 AS SMALLINT) AS version_count,
    
    -- Lineage (compressed)
    CAST(source_file_hash AS VARCHAR(64)) AS source_hash,
    CAST(row_hash AS VARCHAR(32)) AS row_hash,
    
    -- Silver metadata
    CAST('{transform_version}' AS VARCHAR(10)) AS transform_version,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at,
    CAST('{snapshot_id}' AS VARCHAR(50)) AS snapshot_id
    
  FROM deduplicated ded
  JOIN organization_dim o ON ded."Organization" = o.organization_name
  JOIN department_dim d ON ded."Department" = d.department_name
  JOIN status_dim s ON ded."Status" = s.status_code
)

SELECT * FROM transformed
WHERE 1=1
  -- Data quality filters
  AND po_id IS NOT NULL
  AND sent_date >= DATE '2018-01-31'  -- Nevada ePro start date
  AND sent_date <= CURRENT_DATE + INTERVAL 30 DAY
  AND total_amount >= 0
  -- Remove test data
  AND po_id NOT SIMILAR TO '%(TEST|DEMO|TEMP|SAMPLE)%'
  AND vendor_name NOT SIMILAR TO '%(TEST|DEMO|DO NOT USE)%'
ORDER BY sent_date DESC, po_id