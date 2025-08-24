import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Read the HHI results
df = pd.read_csv('output/vendor_concentration_results.csv')
print(f"Data shape: {df.shape}")
print(f"HHI range: {df['hhi'].min():.3f} to {df['hhi'].max():.3f}")

# Simple histogram
plt.figure(figsize=(10, 6))
plt.hist(df['hhi'], bins=15, alpha=0.7, edgecolor='black')
plt.axvline(0.15, color='orange', linestyle='--', label='Moderate threshold')
plt.axvline(0.25, color='red', linestyle='--', label='High threshold')
plt.xlabel('HHI')
plt.ylabel('Frequency')
plt.title('Vendor Concentration Distribution')
plt.legend()
plt.savefig('output/hhi_test_plot.png', dpi=150, bbox_inches='tight')
plt.close()
print("Test plot saved to output/hhi_test_plot.png")