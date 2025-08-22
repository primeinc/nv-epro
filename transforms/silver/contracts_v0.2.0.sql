-- Silver transformation for contracts dataset v0.2.0
-- Uses EXACT data types from profiling
-- Implements deduplication and business logic
-- Generated from exact-data-types.json

WITH bronze_data AS (
  SELECT * FROM {bronze_table}
),

-- Deduplicate by taking the latest version of each contract
latest_per_contract AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY "Contract #" 
      ORDER BY ingested_at DESC, source_row DESC
    ) AS rn
  FROM bronze_data
),

deduplicated AS (
  SELECT * FROM latest_per_contract WHERE rn = 1
),

transformed AS (
  SELECT
    -- Primary key (100% unique)
    CAST("Contract #" AS VARCHAR(28)) AS contract_id,
    
    -- Related identifiers (all null in current data but keeping for future)
    CAST("Bid Solicitation #" AS VARCHAR(50)) AS bid_solicitation_id,
    CAST("Type Code" AS VARCHAR(50)) AS type_code,
    
    -- Core contract details with exact types
    CAST("Description" AS VARCHAR(132)) AS description,
    CAST("Vendor" AS VARCHAR(108)) AS vendor_name,
    CAST("Organization" AS VARCHAR(63)) AS organization,
    
    -- Financial tracking
    CAST(TRY_CAST(REPLACE(REPLACE("Dollars Spent to Date", '$', ''), ',', '') AS DECIMAL(12,2)) AS DECIMAL(12,2)) AS dollars_spent_to_date,
    
    -- Contract dates
    CAST(strptime("Begin Date", '%m/%d/%Y') AS DATE) AS begin_date,
    CAST(strptime("End Date", '%m/%d/%Y') AS DATE) AS end_date,
    
    -- Calculate contract duration
    CAST(DATE_DIFF('day', strptime("Begin Date", '%m/%d/%Y'), strptime("End Date", '%m/%d/%Y')) AS INTEGER) AS contract_duration_days,
    CAST(DATE_DIFF('month', strptime("Begin Date", '%m/%d/%Y'), strptime("End Date", '%m/%d/%Y')) AS INTEGER) AS contract_duration_months,
    
    -- Status (only one value in current data: '3PS - Sent')
    CAST("Status" AS VARCHAR(15)) AS status_code,
    CAST(
      CASE "Status"
        WHEN '3PS - Sent' THEN 'Sent'
        ELSE SPLIT_PART("Status", ' - ', 2)
      END AS VARCHAR(10)
    ) AS status_name,
    
    -- Contract temporal analysis
    CAST(
      CASE
        WHEN CURRENT_DATE < strptime("Begin Date", '%m/%d/%Y') THEN 'Future'
        WHEN CURRENT_DATE > strptime("End Date", '%m/%d/%Y') THEN 'Expired'
        ELSE 'Active'
      END AS VARCHAR(10)
    ) AS contract_status,
    
    -- Days until expiration (negative if expired)
    CAST(DATE_DIFF('day', CURRENT_DATE, strptime("End Date", '%m/%d/%Y')) AS INTEGER) AS days_until_expiration,
    
    -- Fiscal year for begin date (Nevada FY starts July 1)
    CAST(
      CASE
        WHEN MONTH(strptime("Begin Date", '%m/%d/%Y')) >= 7
        THEN YEAR(strptime("Begin Date", '%m/%d/%Y')) + 1
        ELSE YEAR(strptime("Begin Date", '%m/%d/%Y'))
      END AS SMALLINT
    ) AS fiscal_year_begin,
    
    -- Fiscal year for end date
    CAST(
      CASE
        WHEN MONTH(strptime("End Date", '%m/%d/%Y')) >= 7
        THEN YEAR(strptime("End Date", '%m/%d/%Y')) + 1
        ELSE YEAR(strptime("End Date", '%m/%d/%Y'))
      END AS SMALLINT
    ) AS fiscal_year_end,
    
    -- Analytical flags
    CAST(TRY_CAST(REPLACE(REPLACE("Dollars Spent to Date", '$', ''), ',', '') AS DECIMAL(12,2)) = 0 AS BOOLEAN) AS is_zero_spend,
    CAST("Organization" = 'Statewide Contracts' AS BOOLEAN) AS is_statewide_contract,
    CAST(DATE_DIFF('year', strptime("Begin Date", '%m/%d/%Y'), strptime("End Date", '%m/%d/%Y')) > 1 AS BOOLEAN) AS is_multi_year,
    
    -- Spending rate calculation (dollars per day if contract is active/expired)
    CAST(
      CASE
        WHEN CURRENT_DATE > strptime("Begin Date", '%m/%d/%Y') THEN
          TRY_CAST(REPLACE(REPLACE("Dollars Spent to Date", '$', ''), ',', '') AS DECIMAL(12,2)) / 
          NULLIF(DATE_DIFF('day', 
            strptime("Begin Date", '%m/%d/%Y'),
            LEAST(CURRENT_DATE, strptime("End Date", '%m/%d/%Y'))
          ), 0)
        ELSE NULL
      END AS DECIMAL(12,2)
    ) AS daily_spend_rate,
    
    -- Vendor categorization
    CAST(
      CASE
        WHEN "Vendor" ILIKE '%amazon%' THEN 'Amazon'
        WHEN "Vendor" ILIKE '%staples%' THEN 'Staples'
        WHEN "Vendor" ILIKE '%dell%' THEN 'Dell'
        WHEN "Vendor" ILIKE '%grainger%' THEN 'Grainger'
        WHEN "Vendor" ILIKE '%cdw%' THEN 'CDW'
        WHEN "Vendor" ILIKE '%office depot%' OR "Vendor" ILIKE '%ODP%' THEN 'Office Depot'
        WHEN "Vendor" ILIKE '%microsoft%' THEN 'Microsoft'
        WHEN "Vendor" ILIKE '%oracle%' THEN 'Oracle'
        ELSE 'Other'
      END AS VARCHAR(20)
    ) AS vendor_category,
    
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
  AND contract_id IS NOT NULL
  AND begin_date IS NOT NULL
  AND end_date IS NOT NULL
  AND begin_date <= end_date  -- Begin date must be before end date
  AND begin_date >= DATE '2018-01-31'  -- Nevada ePro start date
  AND end_date <= DATE '2050-12-31'  -- Reasonable future limit
  AND dollars_spent_to_date >= 0  -- No negative spending
  -- Remove test data
  AND contract_id NOT ILIKE '%TEST%'
  AND vendor_name NOT ILIKE '%TEST%'
  AND vendor_name NOT ILIKE '%DO NOT USE%'
ORDER BY begin_date DESC, contract_id