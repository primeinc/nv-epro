import * as duckdb from '@duckdb/duckdb-wasm';
import type { 
  DashboardMetrics, 
  VendorSummary, 
  MonthlySummary, 
  DepartmentSummary, 
  StatusSummary,
  VendorDetail,
  PurchaseOrder
} from '../types';

export async function calculateMetrics(conn: duckdb.AsyncDuckDBConnection): Promise<DashboardMetrics> {
  const metrics: DashboardMetrics = {};
  
  // Try to get PO metrics
  try {
    const poResult = await conn.query(`
      SELECT 
        COUNT(*) as count,
        SUM(CAST(total_amount AS DOUBLE)) as total,
        AVG(CAST(total_amount AS DOUBLE)) as avg_amount
      FROM purchase_orders
    `);
    const poData = poResult.toArray()[0];
    metrics.poCount = poData?.count || 0;
    metrics.poTotal = poData?.total || 0;
    metrics.avgPOAmount = poData?.avg_amount || 0;
  } catch (e) {
    console.log('No purchase orders data available');
  }
  
  // Try to get contract metrics
  try {
    const contractResult = await conn.query(`
      SELECT COUNT(*) as count
      FROM contracts
    `);
    metrics.contractCount = contractResult.toArray()[0]?.count || 0;
  } catch (e) {
    console.log('No contracts data available');
  }
  
  // Try to get vendor metrics
  try {
    const vendorResult = await conn.query(`
      SELECT COUNT(DISTINCT vendor_name) as count
      FROM vendors
    `);
    metrics.vendorCount = vendorResult.toArray()[0]?.count || 0;
  } catch (e) {
    console.log('No vendors data available');
  }
  
  // Get active vs complete POs
  try {
    const statusResult = await conn.query(`
      SELECT 
        CAST(SUM(CASE WHEN status_category = 'Sent' THEN 1 ELSE 0 END) AS DOUBLE) as active,
        CAST(SUM(CASE WHEN status_category IN ('Complete', 'Closed') THEN 1 ELSE 0 END) AS DOUBLE) as completed
      FROM purchase_orders
    `);
    const statusData = statusResult.toArray()[0];
    metrics.activePOs = Number(statusData?.active) || 0;
    metrics.completedPOs = Number(statusData?.completed) || 0;
  } catch (e) {
    console.log('No status data available');
  }
  
  return metrics;
}

export async function getTopVendors(conn: duckdb.AsyncDuckDBConnection): Promise<VendorSummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        vendor_name as vendor,
        COUNT(*) as count,
        SUM(CAST(total_amount AS DOUBLE)) as total
      FROM purchase_orders
      WHERE vendor_name IS NOT NULL
      GROUP BY vendor_name
      ORDER BY total DESC
      LIMIT 10
    `);
    return result.toArray() as VendorSummary[];
  } catch (e) {
    console.log('Could not get top vendors');
    return [];
  }
}

export async function getMonthlyTrend(conn: duckdb.AsyncDuckDBConnection): Promise<MonthlySummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        DATE_TRUNC('month', sent_date) as month,
        COUNT(*) as count,
        SUM(CAST(total_amount AS DOUBLE)) as total
      FROM purchase_orders
      WHERE sent_date IS NOT NULL
      GROUP BY month
      ORDER BY month DESC
      LIMIT 12
    `);
    return result.toArray().map((row: any) => ({
      month: new Date(row.month).toLocaleDateString('en-US', { year: 'numeric', month: 'short' }),
      count: row.count,
      total: row.total
    }));
  } catch (e) {
    console.log('Could not get monthly trend');
    return [];
  }
}

export async function getTopDepartments(conn: duckdb.AsyncDuckDBConnection): Promise<DepartmentSummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        department,
        CAST(COUNT(*) AS DOUBLE) as count,
        CAST(SUM(CAST(total_amount AS DOUBLE)) AS DOUBLE) as total
      FROM purchase_orders
      WHERE department IS NOT NULL
      GROUP BY department
      ORDER BY total DESC
      LIMIT 5
    `);
    return result.toArray() as DepartmentSummary[];
  } catch (e) {
    console.log('Could not get department data');
    return [];
  }
}

export async function getStatusDistribution(conn: duckdb.AsyncDuckDBConnection): Promise<StatusSummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        status_category as status,
        CAST(COUNT(*) AS DOUBLE) as count
      FROM purchase_orders
      WHERE status_category IS NOT NULL
      GROUP BY status_category
      ORDER BY count DESC
    `);
    return result.toArray() as StatusSummary[];
  } catch (e) {
    console.log('Could not get status distribution');
    return [];
  }
}

// Vendor detail queries
export async function getVendorSummary(conn: duckdb.AsyncDuckDBConnection, vendorName: string): Promise<VendorDetail | null> {
  try {
    const result = await conn.query(`
      SELECT 
        vendor_name,
        COUNT(*) as po_count,
        CAST(SUM(CAST(total_amount AS DOUBLE)) AS DOUBLE) as total_amount,
        CAST(AVG(CAST(total_amount AS DOUBLE)) AS DOUBLE) as avg_amount,
        MIN(sent_date) as first_po,
        MAX(sent_date) as last_po
      FROM purchase_orders
      WHERE vendor_name = '${vendorName.replace(/'/g, "''")}'
      GROUP BY vendor_name
    `);
    
    const data = result.toArray()[0];
    return data ? {
      vendor_name: data.vendor_name,
      po_count: Number(data.po_count),
      total_amount: Number(data.total_amount),
      avg_amount: Number(data.avg_amount),
      first_po: data.first_po,
      last_po: data.last_po
    } : null;
  } catch (e) {
    console.error('Error getting vendor summary:', e);
    return null;
  }
}

export async function getVendorRecentPOs(conn: duckdb.AsyncDuckDBConnection, vendorName: string): Promise<PurchaseOrder[]> {
  try {
    const result = await conn.query(`
      SELECT 
        po_id,
        sent_date,
        CAST(total_amount AS DOUBLE) as total_amount,
        department,
        buyer_name,
        status_category,
        description
      FROM purchase_orders
      WHERE vendor_name = '${vendorName.replace(/'/g, "''")}'
      ORDER BY sent_date DESC
      LIMIT 20
    `);
    
    return result.toArray().map((row: any) => ({
      po_id: row.po_id,
      sent_date: row.sent_date,
      total_amount: Number(row.total_amount),
      department: row.department,
      buyer_name: row.buyer_name,
      status_category: row.status_category,
      description: row.description
    }));
  } catch (e) {
    console.error('Error getting recent POs:', e);
    return [];
  }
}

export async function getVendorDepartments(conn: duckdb.AsyncDuckDBConnection, vendorName: string): Promise<DepartmentSummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        department,
        COUNT(*) as count,
        CAST(SUM(CAST(total_amount AS DOUBLE)) AS DOUBLE) as total
      FROM purchase_orders
      WHERE vendor_name = '${vendorName.replace(/'/g, "''")}' AND department IS NOT NULL
      GROUP BY department
      ORDER BY total DESC
      LIMIT 10
    `);
    
    return result.toArray().map((row: any) => ({
      department: row.department,
      count: Number(row.count),
      total: Number(row.total)
    }));
  } catch (e) {
    console.error('Error getting vendor departments:', e);
    return [];
  }
}

export async function getVendorsByLatestPO(conn: duckdb.AsyncDuckDBConnection, limit: number = 50, offset: number = 0): Promise<any[]> {
  try {
    const result = await conn.query(`
      WITH vendor_latest AS (
        SELECT 
          vendor_name,
          MAX(sent_date) as latest_po_date,
          COUNT(*) as total_pos,
          CAST(SUM(CAST(total_amount AS DOUBLE)) AS DOUBLE) as total_amount,
          CAST(AVG(CAST(total_amount AS DOUBLE)) AS DOUBLE) as avg_amount
        FROM purchase_orders
        WHERE vendor_name IS NOT NULL AND sent_date IS NOT NULL
        GROUP BY vendor_name
      )
      SELECT 
        v.vendor_name,
        v.latest_po_date,
        v.total_pos,
        v.total_amount,
        v.avg_amount,
        p.po_id as latest_po_id,
        CAST(p.total_amount AS DOUBLE) as latest_po_amount,
        p.status_category as latest_po_status,
        p.department as latest_department
      FROM vendor_latest v
      LEFT JOIN purchase_orders p ON 
        p.vendor_name = v.vendor_name AND 
        p.sent_date = v.latest_po_date
      ORDER BY v.latest_po_date DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);
    
    return result.toArray().map((row: any) => ({
      vendor: row.vendor_name,
      latestPODate: row.latest_po_date,
      totalPOs: typeof row.total_pos === 'bigint' ? Number(row.total_pos) : Number(row.total_pos),
      totalAmount: Number(row.total_amount),
      avgAmount: Number(row.avg_amount),
      latestPOId: row.latest_po_id,
      latestPOAmount: Number(row.latest_po_amount),
      latestPOStatus: row.latest_po_status,
      latestDepartment: row.latest_department
    }));
  } catch (e) {
    console.error('Error getting vendors by latest PO:', e);
    return [];
  }
}

export async function getTotalVendorCount(conn: duckdb.AsyncDuckDBConnection): Promise<number> {
  try {
    const result = await conn.query(`
      SELECT CAST(COUNT(DISTINCT vendor_name) AS DOUBLE) as count
      FROM purchase_orders
      WHERE vendor_name IS NOT NULL
    `);
    return Number(result.toArray()[0]?.count) || 0;
  } catch (e) {
    console.error('Error getting vendor count:', e);
    return 0;
  }
}

export async function getVendorMonthlyTrend(conn: duckdb.AsyncDuckDBConnection, vendorName: string): Promise<MonthlySummary[]> {
  try {
    const result = await conn.query(`
      SELECT 
        DATE_TRUNC('month', sent_date) as month,
        COUNT(*) as count,
        CAST(SUM(CAST(total_amount AS DOUBLE)) AS DOUBLE) as total
      FROM purchase_orders
      WHERE vendor_name = '${vendorName.replace(/'/g, "''")}' AND sent_date IS NOT NULL
      GROUP BY month
      ORDER BY month DESC
      LIMIT 12
    `);
    
    return result.toArray().map((row: any) => ({
      month: new Date(row.month).toLocaleDateString('en-US', { year: 'numeric', month: 'short' }),
      count: Number(row.count),
      total: Number(row.total)
    }));
  } catch (e) {
    console.error('Error getting vendor monthly trend:', e);
    return [];
  }
}