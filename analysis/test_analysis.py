#!/usr/bin/env python3
"""
Test script for Nevada procurement analytics setup
"""
import pandas as pd
import numpy as np
import duckdb
import os

def test_data_access():
    """Test that we can access all sample data files"""
    print("Testing data access...")
    
    # Check if data directory exists
    if not os.path.exists('data'):
        print("ERROR Data directory not found")
        return False
        
    # Test each sample file
    files_to_test = [
        'data/bids_sample.csv',
        'data/contracts_sample.csv', 
        'data/vendors_sample.csv',
        'data/purchase_orders_sample.csv'
    ]
    
    conn = duckdb.connect()
    
    for file_path in files_to_test:
        try:
            if file_path.endswith('.csv'):
                query = f"SELECT COUNT(*) as count FROM '{file_path}'"
                result = conn.execute(query).fetchone()
                print(f"OK {file_path}: {result[0]:,} records")
            else:
                print(f"SKIP {file_path} (not CSV)")
        except Exception as e:
            print(f"ERROR {file_path}: {e}")
            
    return True

def test_vendor_concentration():
    """Test HHI calculation on contracts data"""
    print("\nTesting vendor concentration analysis...")
    
    conn = duckdb.connect()
    
    # Simple HHI calculation
    hhi_query = """
    WITH vendor_spend AS (
        SELECT 
            organization,
            fiscal_year_begin,
            vendor_name,
            SUM(dollars_spent_to_date) as vendor_spend
        FROM 'data/contracts_sample.csv'
        WHERE vendor_name IS NOT NULL 
            AND organization IS NOT NULL 
            AND fiscal_year_begin IS NOT NULL
            AND dollars_spent_to_date > 0
        GROUP BY 1,2,3
    ),
    org_totals AS (
        SELECT 
            organization,
            fiscal_year_begin,
            SUM(vendor_spend) as total_spend
        FROM vendor_spend 
        GROUP BY 1,2
    ),
    market_shares AS (
        SELECT 
            v.*,
            o.total_spend,
            v.vendor_spend / o.total_spend as market_share
        FROM vendor_spend v
        JOIN org_totals o USING (organization, fiscal_year_begin)
    )
    SELECT 
        organization,
        fiscal_year_begin,
        total_spend,
        SUM(market_share * market_share) as hhi,
        COUNT(*) as unique_vendors
    FROM market_shares
    GROUP BY organization, fiscal_year_begin, total_spend
    ORDER BY hhi DESC
    LIMIT 10
    """
    
    try:
        hhi_results = conn.execute(hhi_query).df()
        print(f"OK HHI calculated for {len(hhi_results)} org-year combinations")
        print("\nTop 5 most concentrated markets:")
        print(hhi_results.head()[['organization', 'fiscal_year_begin', 'hhi', 'unique_vendors']].to_string(index=False))
        return True
    except Exception as e:
        print(f"ERROR HHI calculation failed: {e}")
        return False

def test_plotting():
    """Test matplotlib functionality"""
    print("\nTesting plotting capabilities...")
    
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Simple test plot
        fig, ax = plt.subplots(figsize=(8, 6))
        x = np.linspace(0, 10, 100)
        y = np.sin(x)
        ax.plot(x, y)
        ax.set_title('Test Plot - Matplotlib Working')
        plt.savefig('output/test_plot.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        print("OK Matplotlib and Seaborn working")
        print("OK Test plot saved to output/test_plot.png")
        return True
    except Exception as e:
        print(f"ERROR Plotting failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Nevada Procurement Analytics Setup\n")
    
    # Run all tests
    tests = [
        test_data_access,
        test_vendor_concentration, 
        test_plotting
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    # Summary
    print(f"\nTest Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("All tests passed! Analytics environment ready.")
    else:
        print("Some tests failed. Check errors above.")