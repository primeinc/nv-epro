import * as duckdb from '@duckdb/duckdb-wasm';

export async function initializeDuckDB() {
  const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
    mvp: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-mvp.wasm',
      mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js', import.meta.url).href,
    },
    eh: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-eh.wasm',
      mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url).href,
    },
  };
  
  const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
  
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  
  const conn = await db.connect();
  
  // Note: In browsers, we use fetch + registerFileBuffer to load Parquet files
  // httpfs is only for Node.js/server environments, not browsers
  
  return { db, conn };
}