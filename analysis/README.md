# Nevada Procurement Data Analysis

## Overview
Analytics and research notebooks for Nevada procurement data, built on stratified research samples designed for LLM and data science analysis.

## Setup

```bash
# Install Python dependencies
pip install -r ../requirements.txt

# Start Jupyter
jupyter lab
```

## Data
- **Source**: `data/` (symlinked from `../data/research/`)
- **Coverage**: 
  - Bids: 1,000 records (37.4% coverage)
  - Contracts: 800 records (49.8% coverage) 
  - Vendors: 2,000 records (10.2% coverage)
  - Purchase Orders: 5,000 records (5.2% coverage)

## Analysis Notebooks

### 📊 Core Analytics
1. **`01-vendor-concentration.ipynb`** - HHI analysis, market concentration, competition risks
2. **`02-bid-competition.ipynb`** - *(Planned)* Bid-to-award conversion, competition signals
3. **`03-anomaly-detection.ipynb`** - *(Planned)* Outlier detection, Benford's Law testing
4. **`04-procurement-cycles.ipynb`** - *(Planned)* Lifecycle analysis, duration patterns

### 📈 Executive Reports
- **`executive-summary.ipynb`** - *(Planned)* High-level insights for policy makers

## Key Metrics & Methods

### Vendor Concentration (HHI)
- **Formula**: HHI = Σ(market_share²)
- **Interpretation**: 
  - < 0.15: Competitive
  - 0.15-0.25: Moderately concentrated  
  - > 0.25: Highly concentrated
- **Data**: Contracts sample (49.8% coverage = reliable for trends)

### Competition Analysis
- **Bid conversion rates**: % bids leading to awards
- **Single-bidder percentage**: Lack of competition indicator
- **Average bidders per solicitation**: Competition intensity

### Statistical Approach
- **Bootstrap sampling** for confidence intervals
- **Stratified analysis** respecting sample design
- **Coverage-weighted interpretation** of results

## Output Files
- `output/` - Charts, CSV exports, summary statistics
- `reports/` - Formatted reports for stakeholders

## Caveats & Limitations

⚠️ **Sample Coverage Warnings**:
- **High reliability**: Contracts (49.8%), Bids (37.4%) 
- **Medium reliability**: Purchase Orders (5.2%) - trends only
- **Low reliability**: Vendors (10.2%) - biased toward major vendors

⚠️ **Missing Data**: No geographic/location information available

⚠️ **Temporal**: Analysis reflects procurement patterns 2018-2025, with stratification by time period

## References
- Herfindahl-Hirschman Index: DOJ Antitrust Guidelines
- Sampling methodology: Cochran's "Sampling Techniques" (1977)
- Benford's Law: Newcomb (1881), Benford (1938)