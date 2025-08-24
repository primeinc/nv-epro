# Nevada Public Procurement Data Pipeline

An automated data pipeline for scraping, cleaning, and archiving public procurement data (bids, contracts, purchase orders, vendors) from the Nevada eProcurement portal (nevadaepro.com).

This project provides a complete, state-aware system to create a clean, analysis-ready dataset of Nevada's public procurement activities.

## Features

- **Automated Scraping**: Uses Playwright to scrape four main datasets: Purchase Orders, Bids, Contracts, and Vendors.
- **Medallion Architecture**: Raw data is ingested into a "Bronze" layer (immutable, content-addressed Parquet files) and then cleaned and transformed into a "Silver" layer for analysis.
- **State-Aware Orchestration**: A powerful orchestrator (`orchestrate.js`) plans and executes scraping tasks based on the current state of the data, ensuring efficiency and idempotency.
- **Data Validation**: Includes a validation suite to check the quality and integrity of the Bronze layer data after ingestion.
- **CI/CD Automation**: The entire pipeline is automated with GitHub Actions, running on a daily schedule to fetch the latest data.
- **Tooling**: Comes with a suite of tools for monitoring data ranges, analyzing CSVs, and validating data integrity.

## Getting Started

### Prerequisites

- Node.js (v22 is used in the pipeline)
- pnpm

### Installation

1. **Clone the repository:**

   Bash

   ```
   git clone https://github.com/primeinc/nevada-public-procurement.git
   cd nevada-public-procurement
   ```

2. **Install dependencies:**

   Bash

   ```
   pnpm install --frozen-lockfile
   ```

3. Install Playwright browsers:

   The scraper uses Playwright with Chromium to interact with the website.

   Bash

   ```
   pnpm exec playwright install --with-deps chromium
   ```

## Usage

The pipeline can be run in several ways, from individual scrapers to the fully automated orchestrator.

### Running the Full Pipeline

This is the recommended way to run the pipeline. It will automatically check for configuration updates, determine which scraping tasks need to be run, execute them, and then process the data through the Bronze and Silver layers.

Bash

```
pnpm run pipeline
```

### Running Individual Scrapers

You can also run the scrapers for each dataset individually.

- **Purchase Orders (by date range):**

  Bash

  ```
  # Scrape for a specific month and year
  pnpm run po aug 2025
  
  # Scrape for a specific day
  pnpm run po aug 23 2025
  ```

- **Bids (all):**

  Bash

  ```
  pnpm run bid
  ```

- **Contracts (all):**

  Bash

  ```
  pnpm run contract
  ```

- **Vendors (all):**

  Bash

  ```
  pnpm run vendor
  ```

### Data Ingestion and Transformation

After running the scrapers, you can manually trigger the ingestion and validation steps.

- **Ingest raw data to Bronze layer:**

  Bash

  ```
  pnpm run bronze:ingest
  ```

- **Validate Bronze layer data:**

  Bash

  ```
  pnpm run bronze:validate
  ```

- **Transform Bronze data to Silver layer:**

  Bash

  ```
  pnpm run silver:ingest
  ```

## Project Structure

```
.
├── .github/workflows/  # GitHub Actions CI/CD pipelines
├── config/             # Configuration files for schemas, PO ranges, etc.
├── data/               # Root directory for all scraped and processed data
│   ├── bronze/         # Immutable, content-addressed raw data (Parquet)
│   ├── silver/         # Cleaned, transformed data (Parquet)
│   └── nevada-epro/    # Raw scraped data (CSVs, logs, etc.)
├── lib/                # Core library code for ingestion, scraping, etc.
├── scripts/            # CLI scripts for scraping and data processing
└── transforms/         # SQL files for transforming Bronze to Silver data
```

## Contributing

Contributions are welcome! Please open an issue to discuss your ideas or submit a pull request.