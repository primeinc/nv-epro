import * as duckdb from '@duckdb/duckdb-wasm';
import type { VendorSummary, PurchaseOrder } from '../types';

export interface SearchResults {
  vendors: VendorSummary[];
  purchaseOrders: PurchaseOrder[];
  totalResults: number;
}

export async function searchAll(
  conn: duckdb.AsyncDuckDBConnection, 
  searchTerm: string
): Promise<SearchResults> {
  if (!searchTerm) {
    return { vendors: [], purchaseOrders: [], totalResults: 0 };
  }
  
  const escapedTerm = searchTerm.replace(/'/g, "''");
  
  // Search vendors
  const vendorResults = await searchVendors(conn, searchTerm);
  
  // Search POs
  const poResults = await searchPurchaseOrders(conn, searchTerm);
  
  return {
    vendors: vendorResults.slice(0, 10),
    purchaseOrders: poResults.slice(0, 20),
    totalResults: vendorResults.length + poResults.length
  };
}

export async function searchVendors(
  conn: duckdb.AsyncDuckDBConnection,
  searchTerm: string
): Promise<VendorSummary[]> {
  if (!searchTerm) return [];
  
  const escapedTerm = searchTerm.replace(/'/g, "''");
  
  try {
    const result = await conn.query(`
      SELECT 
        vendor_name as vendor,
        COUNT(*) as count,
        SUM(CAST(total_amount AS DOUBLE)) as total
      FROM purchase_orders
      WHERE vendor_name ILIKE '%${escapedTerm}%'
      GROUP BY vendor_name
      ORDER BY total DESC
      LIMIT 50
    `);
    
    return result.toArray().map((row: any) => ({
      vendor: row.vendor,
      count: Number(row.count),
      total: Number(row.total)
    }));
  } catch (e) {
    console.error('Error searching vendors:', e);
    return [];
  }
}

export async function searchPurchaseOrders(
  conn: duckdb.AsyncDuckDBConnection,
  searchTerm: string
): Promise<PurchaseOrder[]> {
  if (!searchTerm) return [];
  
  const escapedTerm = searchTerm.replace(/'/g, "''");
  
  try {
    const result = await conn.query(`
      SELECT 
        po_id,
        sent_date,
        CAST(total_amount AS DOUBLE) as total_amount,
        department,
        buyer_name,
        status_category,
        description,
        vendor_name
      FROM purchase_orders
      WHERE po_id ILIKE '%${escapedTerm}%'
         OR vendor_name ILIKE '%${escapedTerm}%'
         OR department ILIKE '%${escapedTerm}%'
         OR buyer_name ILIKE '%${escapedTerm}%'
         OR description ILIKE '%${escapedTerm}%'
      ORDER BY sent_date DESC
      LIMIT 100
    `);
    
    return result.toArray().map((row: any) => ({
      po_id: row.po_id,
      sent_date: row.sent_date,
      total_amount: Number(row.total_amount),
      department: row.department,
      buyer_name: row.buyer_name,
      status_category: row.status_category,
      description: row.description,
      vendor_name: row.vendor_name
    }));
  } catch (e) {
    console.error('Error searching purchase orders:', e);
    return [];
  }
}

export async function searchByDepartment(
  conn: duckdb.AsyncDuckDBConnection,
  department: string
): Promise<PurchaseOrder[]> {
  if (!department) return [];
  
  const escapedDept = department.replace(/'/g, "''");
  
  try {
    const result = await conn.query(`
      SELECT 
        po_id,
        sent_date,
        CAST(total_amount AS DOUBLE) as total_amount,
        department,
        buyer_name,
        status_category,
        description,
        vendor_name
      FROM purchase_orders
      WHERE department ILIKE '%${escapedDept}%'
      ORDER BY sent_date DESC
      LIMIT 100
    `);
    
    return result.toArray().map((row: any) => ({
      po_id: row.po_id,
      sent_date: row.sent_date,
      total_amount: Number(row.total_amount),
      department: row.department,
      buyer_name: row.buyer_name,
      status_category: row.status_category,
      description: row.description,
      vendor_name: row.vendor_name
    }));
  } catch (e) {
    console.error('Error searching by department:', e);
    return [];
  }
}

export async function searchByPOId(
  conn: duckdb.AsyncDuckDBConnection,
  poId: string
): Promise<PurchaseOrder | null> {
  if (!poId) return null;
  
  const escapedId = poId.replace(/'/g, "''");
  
  try {
    const result = await conn.query(`
      SELECT 
        po_id,
        sent_date,
        CAST(total_amount AS DOUBLE) as total_amount,
        department,
        buyer_name,
        status_category,
        description,
        vendor_name
      FROM purchase_orders
      WHERE po_id = '${escapedId}'
      LIMIT 1
    `);
    
    const data = result.toArray()[0];
    if (!data) return null;
    
    return {
      po_id: data.po_id,
      sent_date: data.sent_date,
      total_amount: Number(data.total_amount),
      department: data.department,
      buyer_name: data.buyer_name,
      status_category: data.status_category,
      description: data.description,
      vendor_name: data.vendor_name
    };
  } catch (e) {
    console.error('Error searching by PO ID:', e);
    return null;
  }
}