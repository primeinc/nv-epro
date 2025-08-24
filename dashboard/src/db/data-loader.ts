import * as duckdb from '@duckdb/duckdb-wasm';
import type { ManifestData } from '../types';

export async function loadManifest(): Promise<ManifestData> {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}manifest.json`);
    return await response.json();
  } catch (error) {
    console.warn('No manifest found, using empty dataset');
    return { parquet: [] };
  }
}

export async function createViews(db: duckdb.AsyncDuckDB, conn: duckdb.AsyncDuckDBConnection, files: string[]) {
  const datasets = ['purchase_orders', 'contracts', 'bids', 'vendors'];
  
  for (const dataset of datasets) {
    const datasetFiles = files.filter(f => f.includes(`/${dataset}.parquet`));
    if (datasetFiles.length > 0) {
      // In browsers, we fetch files and register them with DuckDB
      console.log(`Loading ${dataset} data...`);
      
      for (let i = 0; i < datasetFiles.length; i++) {
        const file = datasetFiles[i];
        const url = `${import.meta.env.BASE_URL}${file}`;
        const response = await fetch(url);
        const buffer = await response.arrayBuffer();
        await db.registerFileBuffer(`${dataset}_${i}.parquet`, new Uint8Array(buffer));
      }
      
      const unions = datasetFiles
        .map((_, i) => `SELECT * FROM read_parquet('${dataset}_${i}.parquet')`)
        .join(' UNION ALL ');
      
      await conn.query(`CREATE OR REPLACE VIEW ${dataset} AS ${unions}`);
      console.log(`Created view for ${dataset} with ${datasetFiles.length} files`);
    }
  }
}