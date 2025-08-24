import * as duckdb from '@duckdb/duckdb-wasm';

import type { PurchaseOrder, POStatus } from './types';
import type { DataSource } from './dataSource';
import type { Filters } from './state';

/**
 * DuckDB-WASM data source that reads from local Parquet files
 * Based on working implementation from dashboard_old
 * Supports pushing filters to SQL for efficiency
 */
export class DuckDBDataSource implements DataSource {
  private db: duckdb.AsyncDuckDB | null = null;
  private conn: duckdb.AsyncDuckDBConnection | null = null;
  
  constructor(
    private parquetPath: string = 'data/silver/purchase_orders.parquet'
  ) {}

  private async initialize() {
    if (this.db && this.conn) return;

    try {
      // Use CDN bundles like the working implementation
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
      
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      
      this.conn = await this.db.connect();
      
      // Fetch and register the Parquet file like the working implementation
      console.log(`Loading purchase orders from ${this.parquetPath}...`);
      const response = await fetch(`/${this.parquetPath}`);
      if (!response.ok) {
        throw new Error(`Failed to fetch ${this.parquetPath}: ${response.statusText}`);
      }
      const buffer = await response.arrayBuffer();
      await this.db.registerFileBuffer('purchase_orders.parquet', new Uint8Array(buffer));
      
      console.log('DuckDB initialized and file registered');
    } catch (error) {
      console.error('DuckDB initialization failed:', error);
      throw error;
    }
  }

  async load(filters?: Partial<Filters>): Promise<PurchaseOrder[]> {
    try {
      await this.initialize();
      
      if (!this.conn) {
        throw new Error('DuckDB connection not initialized');
      }

      // Build WHERE clause based on filters
      const whereClauses: string[] = [
        'total_amount > 0',
        'sent_date IS NOT NULL'
      ];
      const params: any[] = [];

      if (filters) {
        if (filters.date?.from) {
          whereClauses.push('sent_date >= ?');
          params.push(filters.date.from);
        }
        if (filters.date?.to) {
          whereClauses.push('sent_date <= ?');
          params.push(filters.date.to);
        }
        if (filters.status && filters.status !== 'All') {
          whereClauses.push('status_category = ?');
          params.push(filters.status);
        }
        if (filters.department && filters.department !== 'All') {
          whereClauses.push('department = ?');
          params.push(filters.department);
        }
        if (filters.vendor && filters.vendor !== 'All') {
          whereClauses.push('vendor_name = ?');
          params.push(filters.vendor);
        }
        if (filters.query) {
          // Search across multiple fields
          whereClauses.push(`(
            po_id ILIKE ? OR 
            vendor_name ILIKE ? OR 
            department ILIKE ? OR 
            buyer_name ILIKE ? OR 
            description ILIKE ?
          )`);
          const searchPattern = `%${filters.query}%`;
          params.push(searchPattern, searchPattern, searchPattern, searchPattern, searchPattern);
        }
      }

      // Query using read_parquet function with the registered file
      // No LIMIT - load all matching data
      const query = `
        SELECT 
          po_id,
          vendor_name,
          sent_date::VARCHAR as sent_date,
          total_amount::DOUBLE as total_amount,
          department,
          buyer_name,
          status_category,
          description
        FROM read_parquet('purchase_orders.parquet')
        WHERE ${whereClauses.join(' AND ')}
        ORDER BY sent_date DESC
      `;

      // Build the actual query with filters
      // Use string interpolation carefully with proper escaping
      let finalQuery = `
        SELECT 
          po_id,
          vendor_name,
          sent_date::VARCHAR as sent_date,
          total_amount::DOUBLE as total_amount,
          department,
          buyer_name,
          status_category,
          description
        FROM read_parquet('purchase_orders.parquet')
        WHERE ${whereClauses.join(' AND ')}
        ORDER BY sent_date DESC
      `;

      // For filters that need values, build them safely
      if (filters) {
        const conditions: string[] = ['total_amount > 0', 'sent_date IS NOT NULL'];
        
        if (filters.date?.from) {
          conditions.push(`sent_date >= '${filters.date.from}'`);
        }
        if (filters.date?.to) {
          conditions.push(`sent_date <= '${filters.date.to}'`);
        }
        if (filters.status && filters.status !== 'All') {
          conditions.push(`status_category = '${filters.status.replace(/'/g, "''")}'`);
        }
        if (filters.department && filters.department !== 'All') {
          conditions.push(`department = '${filters.department.replace(/'/g, "''")}'`);
        }
        if (filters.vendor && filters.vendor !== 'All') {
          conditions.push(`vendor_name = '${filters.vendor.replace(/'/g, "''")}'`);
        }
        if (filters.query) {
          const escaped = filters.query.replace(/'/g, "''");
          conditions.push(`(
            po_id ILIKE '%${escaped}%' OR 
            vendor_name ILIKE '%${escaped}%' OR 
            department ILIKE '%${escaped}%' OR 
            buyer_name ILIKE '%${escaped}%' OR 
            description ILIKE '%${escaped}%'
          )`);
        }
        
        finalQuery = `
          SELECT 
            po_id,
            vendor_name,
            sent_date::VARCHAR as sent_date,
            total_amount::DOUBLE as total_amount,
            department,
            buyer_name,
            status_category,
            description
          FROM read_parquet('purchase_orders.parquet')
          WHERE ${conditions.join(' AND ')}
          ORDER BY sent_date DESC
        `;
      }

      const result = await this.conn.query(finalQuery);
      const rows = result.toArray();
      
      console.log(`Loaded ${rows.length} purchase orders from DuckDB`);
      
      return rows.map(row => ({
        po_id: String(row.po_id || ''),
        vendor_name: String(row.vendor_name || 'Unknown'),
        sent_date: String(row.sent_date || '').split('T')[0],
        total_amount: Number(row.total_amount) || 0,
        department: row.department ? String(row.department) : null,
        buyer_name: row.buyer_name ? String(row.buyer_name) : null,
        status_category: row.status_category as any || 'Sent',
        description: row.description ? String(row.description) : null
      }));
    } catch (error) {
      console.error('DuckDB query failed:', error);
      
      // Return empty array to show UI even if data load fails
      return [];
    }
  }

  /**
   * Get connection for direct queries
   * Useful for aggregations and complex analytics
   */
  async getConnection() {
    await this.initialize();
    return this.conn;
  }

  async close() {
    if (this.conn) {
      await this.conn.close();
      this.conn = null;
    }
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }
  }
}