import { initializeDuckDB } from './db/connection';
import { loadManifest, createViews } from './db/data-loader';
import { 
  calculateMetrics, 
  getTopVendors, 
  getMonthlyTrend, 
  getTopDepartments, 
  getStatusDistribution,
  getVendorSummary,
  getVendorRecentPOs,
  getVendorDepartments,
  getVendorMonthlyTrend
} from './db/queries';
import { renderDashboard } from './views/DashboardView';
import { renderVendorDetail } from './views/VendorDetailView';
import { setAppState, appState, updateView } from './state';

export async function showDashboard() {
  updateView('dashboard');
  
  try {
    const [metrics, vendors, monthly, departments, statuses] = await Promise.all([
      calculateMetrics(appState.conn),
      getTopVendors(appState.conn),
      getMonthlyTrend(appState.conn),
      getTopDepartments(appState.conn),
      getStatusDistribution(appState.conn)
    ]);
    
    renderDashboard(metrics, vendors, monthly, departments, statuses);
  } catch (error) {
    console.error('Error loading dashboard:', error);
    showError(`Failed to load dashboard: ${error}`);
  }
}

export async function showVendorDetail(vendorName: string) {
  updateView('vendor-detail', vendorName);
  showLoading('Loading vendor details...');
  
  try {
    const [summary, recentPOs, departments, monthlyData] = await Promise.all([
      getVendorSummary(appState.conn, vendorName),
      getVendorRecentPOs(appState.conn, vendorName),
      getVendorDepartments(appState.conn, vendorName),
      getVendorMonthlyTrend(appState.conn, vendorName)
    ]);
    
    if (!summary) {
      throw new Error('Vendor not found');
    }
    
    renderVendorDetail(summary, recentPOs, departments, monthlyData);
  } catch (error) {
    console.error('Error loading vendor details:', error);
    showError(`Failed to load vendor details: ${error}`);
  }
}

function showLoading(message: string = 'Loading...') {
  const app = document.getElementById('app')!;
  app.innerHTML = `
    <div style="padding: 40px; text-align: center;">
      <div style="font-family: system-ui, -apple-system, sans-serif;">
        ${message}
      </div>
    </div>
  `;
}

function showError(message: string) {
  const app = document.getElementById('app')!;
  app.innerHTML = `
    <div style="padding: 40px; text-align: center; color: red;">
      <h1>Error</h1>
      <p>${message}</p>
      ${appState ? `
        <button onclick="window.showDashboard()" style="margin-top: 20px; padding: 8px 16px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer;">
          Back to Dashboard
        </button>
      ` : ''}
    </div>
  `;
}

function showNoData() {
  const app = document.getElementById('app')!;
  app.innerHTML = `
    <div style="padding: 40px; text-align: center;">
      <h1>Nevada Procurement Dashboard</h1>
      <p>No data available yet. The dashboard will populate after the first data pipeline run.</p>
      <p><a href="https://github.com/primeinc/nevada-procurement-data">View Repository</a></p>
    </div>
  `;
}

// Make functions available globally for onclick handlers
(window as any).showDashboard = showDashboard;
(window as any).showVendorDetail = showVendorDetail;

export async function initializeApp() {
  showLoading('Initializing dashboard...');
  
  try {
    // Initialize DuckDB
    const { db, conn } = await initializeDuckDB();
    
    // Store in app state
    setAppState({
      db,
      conn,
      currentView: 'loading'
    });
    
    // Load manifest
    const manifest = await loadManifest();
    console.log(`Found ${manifest.parquet.length} Parquet files`);
    
    if (manifest.parquet.length === 0) {
      showNoData();
      return;
    }
    
    // Create views from Parquet files
    showLoading('Loading data files...');
    await createViews(db, conn, manifest.parquet);
    
    // Show dashboard
    await showDashboard();
    
  } catch (error) {
    console.error('Dashboard initialization error:', error);
    showError(`Failed to initialize dashboard: ${error}`);
  }
}