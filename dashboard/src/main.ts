import * as duckdb from '@duckdb/duckdb-wasm';
import Chart from 'chart.js/auto';

interface ManifestData {
  parquet: string[];
}

interface VendorSummary {
  vendor: string;
  total: number;
  count: number;
}

interface MonthlySummary {
  month: string;
  total: number;
  count: number;
}

interface DepartmentSummary {
  department: string;
  total: number;
  count: number;
}

interface StatusSummary {
  status: string;
  count: number;
}

async function initializeDuckDB() {
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

async function loadManifest(): Promise<ManifestData> {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}manifest.json`);
    return await response.json();
  } catch (error) {
    console.warn('No manifest found, using empty dataset');
    return { parquet: [] };
  }
}

async function createViews(db: duckdb.AsyncDuckDB, conn: duckdb.AsyncDuckDBConnection, files: string[]) {
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

async function calculateMetrics(conn: duckdb.AsyncDuckDBConnection) {
  const metrics: any = {};
  
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

async function getTopVendors(conn: duckdb.AsyncDuckDBConnection): Promise<VendorSummary[]> {
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

async function getMonthlyTrend(conn: duckdb.AsyncDuckDBConnection): Promise<MonthlySummary[]> {
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

async function getTopDepartments(conn: duckdb.AsyncDuckDBConnection): Promise<DepartmentSummary[]> {
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

async function getStatusDistribution(conn: duckdb.AsyncDuckDBConnection): Promise<StatusSummary[]> {
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

function renderDashboard(metrics: any, vendors: VendorSummary[], monthly: MonthlySummary[], departments: DepartmentSummary[], statuses: StatusSummary[]) {
  const app = document.getElementById('app')!;
  
  app.innerHTML = `
    <div style="font-family: system-ui, -apple-system, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px;">
      <h1>Nevada Procurement Dashboard</h1>
      
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin-bottom: 40px;">
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Purchase Orders</h3>
          <div style="font-size: 24px; font-weight: bold;">${(metrics.poCount || 0).toLocaleString()}</div>
          <div style="color: #666; font-size: 14px;">$${((metrics.poTotal || 0) / 1e9).toFixed(2)}B total</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Average PO Size</h3>
          <div style="font-size: 24px; font-weight: bold;">$${((metrics.avgPOAmount || 0) / 1e3).toFixed(1)}K</div>
          <div style="color: #666; font-size: 14px;">Per order</div>
        </div>
        
        <div style="background: #e8f5e9; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Active POs</h3>
          <div style="font-size: 24px; font-weight: bold; color: #4caf50;">${(metrics.activePOs || 0).toLocaleString()}</div>
          <div style="color: #666; font-size: 14px;">Sent status</div>
        </div>
        
        <div style="background: #f3e5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Completed</h3>
          <div style="font-size: 24px; font-weight: bold; color: #9c27b0;">${(metrics.completedPOs || 0).toLocaleString()}</div>
          <div style="color: #666; font-size: 14px;">Complete/Closed</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Contracts</h3>
          <div style="font-size: 24px; font-weight: bold;">${(metrics.contractCount || 0).toLocaleString()}</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Vendors</h3>
          <div style="font-size: 24px; font-weight: bold;">${(metrics.vendorCount || 0).toLocaleString()}</div>
        </div>
      </div>
      
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 40px; margin-bottom: 40px;">
        <div>
          <h2>Top Vendors by Total Amount</h2>
          <canvas id="vendorChart"></canvas>
        </div>
        
        <div>
          <h2>Monthly Purchase Order Trend</h2>
          <canvas id="monthlyChart"></canvas>
        </div>
      </div>
      
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 40px;">
        <div>
          <h2>Top Departments by Spend</h2>
          <canvas id="departmentChart"></canvas>
        </div>
        
        <div>
          <h2>PO Status Distribution</h2>
          <canvas id="statusChart"></canvas>
        </div>
      </div>
      
      <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px;">
        <p>Data source: <a href="https://nevadaepro.com/bso/">Nevada ePro</a> | Repository: <a href="https://github.com/primeinc/nevada-procurement-data">github.com/primeinc/nevada-procurement-data</a></p>
        <p>Last updated: ${new Date().toLocaleString()}</p>
      </div>
    </div>
  `;
  
  // Render vendor chart
  if (vendors.length > 0) {
    new Chart(document.getElementById('vendorChart') as HTMLCanvasElement, {
      type: 'bar',
      data: {
        labels: vendors.map(v => v.vendor.substring(0, 30)),
        datasets: [{
          label: 'Total Amount ($)',
          data: vendors.map(v => v.total),
          backgroundColor: 'rgba(54, 162, 235, 0.5)',
          borderColor: 'rgba(54, 162, 235, 1)',
          borderWidth: 1
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        onClick: (event, elements) => {
          if (elements.length > 0) {
            const index = elements[0].index;
            const vendor = vendors[index];
            // Show vendor details in alert for now - could be enhanced with modal
            alert(`Vendor: ${vendor.vendor}\nTotal: $${(vendor.total / 1e6).toFixed(2)}M\nPO Count: ${vendor.count}`);
            // Could also filter dashboard or navigate to vendor page here
          }
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `$${(context.parsed.x / 1e6).toFixed(2)}M`
            }
          }
        },
        scales: {
          x: {
            ticks: {
              callback: (value) => `$${(Number(value) / 1e6).toFixed(0)}M`
            }
          }
        }
      }
    });
  }
  
  // Render monthly trend chart
  if (monthly.length > 0) {
    new Chart(document.getElementById('monthlyChart') as HTMLCanvasElement, {
      type: 'line',
      data: {
        labels: monthly.reverse().map(m => m.month),
        datasets: [{
          label: 'Total Amount ($)',
          data: monthly.map(m => m.total),
          borderColor: 'rgba(75, 192, 192, 1)',
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          tension: 0.1
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `$${(context.parsed.y / 1e6).toFixed(2)}M`
            }
          }
        },
        scales: {
          y: {
            ticks: {
              callback: (value) => `$${(Number(value) / 1e6).toFixed(0)}M`
            }
          }
        }
      }
    });
  }
  
  // Render department chart
  if (departments.length > 0) {
    new Chart(document.getElementById('departmentChart') as HTMLCanvasElement, {
      type: 'bar',
      data: {
        labels: departments.map(d => d.department.length > 25 ? d.department.substring(0, 25) + '...' : d.department),
        datasets: [{
          label: 'Total Spend ($)',
          data: departments.map(d => d.total),
          backgroundColor: 'rgba(255, 159, 64, 0.5)',
          borderColor: 'rgba(255, 159, 64, 1)',
          borderWidth: 1
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `$${(context.parsed.x / 1e9).toFixed(2)}B`
            }
          }
        },
        scales: {
          x: {
            ticks: {
              callback: (value) => `$${(Number(value) / 1e9).toFixed(1)}B`
            }
          }
        }
      }
    });
  }
  
  // Render status chart (donut)
  if (statuses.length > 0) {
    new Chart(document.getElementById('statusChart') as HTMLCanvasElement, {
      type: 'doughnut',
      data: {
        labels: statuses.map(s => s.status),
        datasets: [{
          data: statuses.map(s => s.count),
          backgroundColor: [
            'rgba(76, 175, 80, 0.7)',   // Sent - Green
            'rgba(33, 150, 243, 0.7)',   // Complete - Blue
            'rgba(156, 39, 176, 0.7)',   // Closed - Purple
            'rgba(255, 193, 7, 0.7)',    // Partial - Yellow
            'rgba(158, 158, 158, 0.7)'   // Other - Gray
          ],
          borderColor: [
            'rgba(76, 175, 80, 1)',
            'rgba(33, 150, 243, 1)',
            'rgba(156, 39, 176, 1)',
            'rgba(255, 193, 7, 1)',
            'rgba(158, 158, 158, 1)'
          ],
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: {
            position: 'right'
          },
          tooltip: {
            callbacks: {
              label: (context) => {
                const total = statuses.reduce((sum, s) => sum + s.count, 0);
                const percent = ((context.parsed / total) * 100).toFixed(1);
                return `${context.label}: ${context.parsed.toLocaleString()} (${percent}%)`;
              }
            }
          }
        }
      }
    });
  }
}

async function main() {
  const app = document.getElementById('app')!;
  app.innerHTML = '<div style="padding: 40px; text-align: center;">Loading dashboard...</div>';
  
  try {
    // Initialize DuckDB
    const { db, conn } = await initializeDuckDB();
    
    // Load manifest
    const manifest = await loadManifest();
    console.log(`Found ${manifest.parquet.length} Parquet files`);
    
    if (manifest.parquet.length === 0) {
      app.innerHTML = `
        <div style="padding: 40px; text-align: center;">
          <h1>Nevada Procurement Dashboard</h1>
          <p>No data available yet. The dashboard will populate after the first data pipeline run.</p>
          <p><a href="https://github.com/primeinc/nevada-procurement-data">View Repository</a></p>
        </div>
      `;
      return;
    }
    
    // Create views from Parquet files
    await createViews(db, conn, manifest.parquet);
    
    // Calculate metrics
    const metrics = await calculateMetrics(conn);
    const vendors = await getTopVendors(conn);
    const monthly = await getMonthlyTrend(conn);
    const departments = await getTopDepartments(conn);
    const statuses = await getStatusDistribution(conn);
    
    // Render dashboard
    renderDashboard(metrics, vendors, monthly, departments, statuses);
    
  } catch (error) {
    console.error('Dashboard error:', error);
    app.innerHTML = `
      <div style="padding: 40px; text-align: center; color: red;">
        <h1>Error Loading Dashboard</h1>
        <p>${error}</p>
      </div>
    `;
  }
}

// Start the app
main();