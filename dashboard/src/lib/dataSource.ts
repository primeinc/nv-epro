import type { PurchaseOrder } from './types';

/**
 * DataSource abstraction so you can swap backends (in-memory JSON, DuckDB WASM, server API)
 */
export interface DataSource {
  load(): Promise<PurchaseOrder[]>;
}

export class InMemoryDataSource implements DataSource {
  constructor(private url: string = '/src/data/sample-pos.json') {}
  async load() {
    const res = await fetch(this.url);
    if (!res.ok) throw new Error('Failed to load sample POS dataset');
    return await res.json() as PurchaseOrder[];
  }
}