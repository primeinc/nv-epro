#!/usr/bin/env python3
"""
Test HHI calculation with silver data
"""
import pandas as pd
import duckdb
import os

def test_hhi_with_silver():
    """Test complete HHI workflow with silver data"""
    print("=== TESTING HHI WITH SILVER DATA ===")
    
    conn = duckdb.connect()
    
    # Step 1: Load data
    print("1. Loading silver contracts data...")
    contracts_query = """
    SELECT 
        contract_id,
        vendor_name,
        organization,
        fiscal_year_begin,
        dollars_spent_to_date,
        is_zero_spend,
        contract_status
    FROM read_parquet("../data/silver/contracts/version=v0.3.0/*/data.parquet")
    WHERE vendor_name IS NOT NULL 
        AND organization IS NOT NULL
        AND fiscal_year_begin IS NOT NULL
    """
    
    contracts = conn.execute(contracts_query).df()
    print(f"OK Loaded {len(contracts):,} records")
    print(f"  Organizations: {contracts['organization'].nunique()}")
    print(f"  Vendors: {contracts['vendor_name'].nunique()}")
    print(f"  Total spend: ${contracts['dollars_spent_to_date'].sum():,.2f}")
    
    # Step 2: Calculate HHI
    print("\n2. Calculating HHI by organization...")
    hhi_query = """
    WITH vendor_spend AS (
        SELECT 
            organization,
            vendor_name,
            SUM(dollars_spent_to_date) as vendor_spend,
            COUNT(*) as contract_count
        FROM read_parquet("../data/silver/contracts/version=v0.3.0/*/data.parquet")
        WHERE vendor_name IS NOT NULL 
            AND organization IS NOT NULL
        GROUP BY 1,2
    ),
    org_totals AS (
        SELECT 
            organization,
            SUM(vendor_spend) as total_spend,
            COUNT(*) as total_contracts
        FROM vendor_spend 
        GROUP BY 1
    ),
    market_shares AS (
        SELECT 
            v.*,
            o.total_spend,
            o.total_contracts,
            CASE 
                WHEN o.total_spend > 0 THEN v.vendor_spend / o.total_spend 
                ELSE 1.0 / COUNT(*) OVER (PARTITION BY v.organization)
            END as market_share,
            DENSE_RANK() OVER (PARTITION BY v.organization ORDER BY v.vendor_spend DESC) as vendor_rank
        FROM vendor_spend v
        JOIN org_totals o USING (organization)
    )
    SELECT 
        organization,
        total_spend,
        total_contracts,
        SUM(market_share * market_share) as hhi,
        SUM(CASE WHEN vendor_rank <= 5 THEN market_share ELSE 0 END) as top5_share,
        COUNT(*) as unique_vendors,
        COUNT(CASE WHEN market_share >= 0.10 THEN 1 END) as vendors_over_10pct
    FROM market_shares
    WHERE total_contracts >= 5
    GROUP BY organization, total_spend, total_contracts
    ORDER BY hhi DESC
    """
    
    hhi_results = conn.execute(hhi_query).df()
    print(f"OK HHI calculated for {len(hhi_results)} organizations")
    
    # Step 3: Interpret results
    print("\n3. Interpreting results...")
    def interpret_hhi(hhi):
        if hhi < 0.15:
            return "Competitive"
        elif hhi < 0.25:
            return "Moderately Concentrated"
        else:
            return "Highly Concentrated"
    
    hhi_results['concentration_level'] = hhi_results['hhi'].apply(interpret_hhi)
    
    print("Concentration Summary:")
    print(hhi_results['concentration_level'].value_counts())
    print()
    print(f"Average HHI: {hhi_results['hhi'].mean():.3f}")
    print(f"Median HHI: {hhi_results['hhi'].median():.3f}")
    print(f"High concentration (>=0.25): {(hhi_results['hhi'] >= 0.25).sum()}/{len(hhi_results)} ({(hhi_results['hhi'] >= 0.25).mean()*100:.1f}%)")
    
    # Step 4: Show top results
    print("\nTop 5 Most Concentrated:")
    top_results = hhi_results.head(5)[['organization', 'hhi', 'unique_vendors', 'top5_share']]
    print(top_results.to_string(index=False))
    
    # Step 5: Export results
    print("\n4. Exporting results...")
    os.makedirs('../output', exist_ok=True)
    hhi_results.to_csv('../output/hhi_test_results.csv', index=False)
    print("OK Results saved to ../output/hhi_test_results.csv")
    
    return True

if __name__ == "__main__":
    try:
        test_hhi_with_silver()
        print("\nSUCCESS: HHI analysis with silver data successful!")
    except Exception as e:
        print(f"\nERROR: HHI analysis failed: {e}")
        raise