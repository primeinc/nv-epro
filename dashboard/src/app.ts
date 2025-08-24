import { initializeDuckDB } from './db/connection';
import { loadManifest, createViews } from './db/data-loader';
import Chart from 'chart.js/auto';
import { 
  calculateMetrics, 
  getTopVendors, 
  getMonthlyTrend, 
  getTopDepartments, 
  getStatusDistribution,
  getVendorSummary,
  getVendorRecentPOs,
  getVendorDepartments,
  getVendorMonthlyTrend,
  getVendorsByLatestPO,
  getTotalVendorCount
} from './db/queries';
import { searchAll, searchVendors, searchPurchaseOrders, searchByDepartment } from './db/search-queries';
import { renderDashboard, renderOverviewContent } from './views/DashboardView';
import { renderVendorDetail } from './views/VendorDetailView';
import { renderSearchResults } from './views/SearchResultsView';
import { renderVendorList } from './views/VendorListView';
import { renderSearchBar, setupSearchHandlers, type SearchOptions } from './components/SearchBar';
import { LoadingSpinner } from './components/LoadingSpinner';
import { ErrorMessage } from './components/ErrorMessage';
import { setAppState, appState, updateView } from './state';

export async function showDashboard() {
  updateView('dashboard');
  
  const app = document.getElementById('app')!;
  app.innerHTML = renderTabs('overview') + '<div id="tab-content"></div>';
  
  showOverviewTab();
}

async function showOverviewTab() {
  const tabContent = document.getElementById('tab-content')!;
  tabContent.innerHTML = LoadingSpinner({ message: 'Loading dashboard...' });
  
  try {
    const [metrics, vendors, monthly, departments, statuses] = await Promise.all([
      calculateMetrics(appState.conn),
      getTopVendors(appState.conn),
      getMonthlyTrend(appState.conn),
      getTopDepartments(appState.conn),
      getStatusDistribution(appState.conn)
    ]);
    
    tabContent.innerHTML = renderOverviewContent(metrics, vendors, monthly, departments, statuses);
    
    setTimeout(() => {
      renderVendorChart(vendors);
      renderMonthlyChart(monthly);
      renderDepartmentChart(departments);
    }, 0);
    
  } catch (error) {
    console.error('Error loading overview:', error);
    tabContent.innerHTML = ErrorMessage({ message: `Failed to load overview: ${error}` });
  }
}

async function showVendorsTab() {
  const tabContent = document.getElementById('tab-content')!;
  loadVendorPage(1);
}

export async function loadVendorPage(page: number) {
  const tabContent = document.getElementById('tab-content')!;
  tabContent.innerHTML = LoadingSpinner({ message: 'Loading vendors...' });
  
  try {
    const pageSize = 50;
    const offset = (page - 1) * pageSize;
    
    const [vendors, totalCount] = await Promise.all([
      getVendorsByLatestPO(appState.conn, pageSize, offset),
      getTotalVendorCount(appState.conn)
    ]);
    
    tabContent.innerHTML = renderVendorList(vendors, totalCount, page, pageSize);
  } catch (error) {
    console.error('Error loading vendors:', error);
    tabContent.innerHTML = ErrorMessage({ message: `Failed to load vendors: ${error}` });
  }
}

function renderTabs(activeTab: string): string {
  return `
    <div class="dashboard-header">
      <div class="dashboard-header-content">
        <h1 class="dashboard-title">Nevada Procurement Dashboard</h1>
        <div class="tab-navigation">
          <button 
            class="tab-btn ${activeTab === 'overview' ? 'active' : ''}" 
            onclick="window.switchTab('overview')"
          >
            Overview
          </button>
          <button 
            class="tab-btn ${activeTab === 'vendors' ? 'active' : ''}" 
            onclick="window.switchTab('vendors')"
          >
            Vendors
          </button>
        </div>
      </div>
    </div>
  `;
}

async function handleSearch(options: SearchOptions) {
  if (!options.searchTerm) {
    showDashboard();
    return;
  }
  
  showLoading(`Searching for "${options.searchTerm}"...`);
  
  try {
    let results;
    
    switch (options.searchType) {
      case 'vendor':
        const vendors = await searchVendors(appState.conn, options.searchTerm);
        results = { vendors, purchaseOrders: [], totalResults: vendors.length };
        break;
      
      case 'po':
        const pos = await searchPurchaseOrders(appState.conn, options.searchTerm);
        results = { vendors: [], purchaseOrders: pos, totalResults: pos.length };
        break;
      
      case 'department':
        const deptPOs = await searchByDepartment(appState.conn, options.searchTerm);
        results = { vendors: [], purchaseOrders: deptPOs, totalResults: deptPOs.length };
        break;
      
      default:
        results = await searchAll(appState.conn, options.searchTerm);
    }
    
    renderSearchResults(results, options.searchTerm);
  } catch (error) {
    console.error('Error searching:', error);
    showError(`Search failed: ${error}`);
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
  app.innerHTML = LoadingSpinner({ message });
}

function showError(message: string) {
  const app = document.getElementById('app')!;
  app.innerHTML = ErrorMessage({ 
    message, 
    showBackButton: !!appState 
  });
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
(window as any).loadVendorPage = loadVendorPage;
(window as any).switchTab = (tab: string) => {
  const app = document.getElementById('app')!;
  app.innerHTML = renderTabs(tab) + '<div id="tab-content"></div>';
  
  if (tab === 'overview') {
    showOverviewTab();
  } else if (tab === 'vendors') {
    showVendorsTab();
  }
};

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

// Chart rendering functions (moved from DashboardView.ts)
export function renderVendorChart(vendors: any[]) {
  if (vendors.length === 0) return;
  
  const canvas = document.getElementById('vendorChart') as HTMLCanvasElement;
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d')!;
  const gradient = ctx.createLinearGradient(0, 0, 400, 0);
  gradient.addColorStop(0, '#6366f1');
  gradient.addColorStop(1, '#8b5cf6');
  
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: vendors.map(v => v.vendor.substring(0, 30)),
      datasets: [{
        label: 'Total Amount ($)',
        data: vendors.map(v => v.total),
        backgroundColor: gradient,
        borderColor: '#6366f1',
        borderWidth: 0,
        borderRadius: 6
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: true,
      onClick: (event: any, elements: any) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const vendor = vendors[index];
          (window as any).showVendorDetail(vendor.vendor);
        }
      },
      onHover: (event: any, activeElements: any) => {
        (event.native?.target as HTMLElement).style.cursor = activeElements.length > 0 ? 'pointer' : 'default';
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(30, 41, 59, 0.95)',
          padding: 12,
          cornerRadius: 8,
          titleFont: { size: 14, weight: '600' },
          bodyFont: { size: 13 },
          callbacks: {
            label: (context: any) => `$${(context.parsed.x / 1e6).toFixed(2)}M`
          }
        }
      },
      scales: {
        x: {
          grid: {
            display: true,
            drawBorder: false,
            color: 'rgba(0, 0, 0, 0.05)'
          },
          ticks: {
            callback: (value: any) => `$${(Number(value) / 1e6).toFixed(0)}M`,
            font: { size: 11 },
            color: '#6b7280'
          }
        },
        y: {
          grid: {
            display: false,
            drawBorder: false
          },
          ticks: {
            font: { size: 11 },
            color: '#374151'
          }
        }
      }
    }
  });
}

export function renderMonthlyChart(monthly: any[]) {
  if (monthly.length === 0) return;
  
  const canvas = document.getElementById('monthlyChart') as HTMLCanvasElement;
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d')!;
  const gradient = ctx.createLinearGradient(0, 0, 0, 400);
  gradient.addColorStop(0, 'rgba(99, 102, 241, 0.3)');
  gradient.addColorStop(1, 'rgba(99, 102, 241, 0.01)');
  
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: monthly.reverse().map(m => m.month),
      datasets: [{
        label: 'Total Amount ($)',
        data: monthly.map(m => m.total),
        borderColor: '#6366f1',
        backgroundColor: gradient,
        borderWidth: 3,
        tension: 0.4,
        fill: true,
        pointRadius: 4,
        pointBackgroundColor: '#ffffff',
        pointBorderColor: '#6366f1',
        pointBorderWidth: 2,
        pointHoverRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      interaction: {
        intersect: false,
        mode: 'index'
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(30, 41, 59, 0.95)',
          padding: 12,
          cornerRadius: 8,
          titleFont: { size: 14, weight: '600' },
          bodyFont: { size: 13 },
          callbacks: {
            label: (context: any) => `$${(context.parsed.y / 1e6).toFixed(2)}M`
          }
        }
      },
      scales: {
        y: {
          grid: {
            display: true,
            drawBorder: false,
            color: 'rgba(0, 0, 0, 0.05)'
          },
          ticks: {
            callback: (value: any) => `$${(Number(value) / 1e6).toFixed(0)}M`,
            font: { size: 11 },
            color: '#6b7280'
          }
        },
        x: {
          grid: {
            display: false,
            drawBorder: false
          },
          ticks: {
            font: { size: 11 },
            color: '#6b7280'
          }
        }
      }
    }
  });
}

export function renderDepartmentChart(departments: any[]) {
  if (departments.length === 0) return;
  
  const canvas = document.getElementById('departmentChart') as HTMLCanvasElement;
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d')!;
  const gradient = ctx.createLinearGradient(0, 0, 400, 0);
  gradient.addColorStop(0, '#f59e0b');
  gradient.addColorStop(1, '#f97316');
  
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: departments.map(d => d.department.length > 30 ? d.department.substring(0, 30) + '...' : d.department),
      datasets: [{
        label: 'Total Spend ($)',
        data: departments.map(d => d.total),
        backgroundColor: gradient,
        borderColor: '#f59e0b',
        borderWidth: 0,
        borderRadius: 6
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(30, 41, 59, 0.95)',
          padding: 12,
          cornerRadius: 8,
          titleFont: { size: 14, weight: '600' },
          bodyFont: { size: 13 },
          callbacks: {
            label: (context: any) => `$${(context.parsed.x / 1e9).toFixed(2)}B`
          }
        }
      },
      scales: {
        x: {
          grid: {
            display: true,
            drawBorder: false,
            color: 'rgba(0, 0, 0, 0.05)'
          },
          ticks: {
            callback: (value: any) => `$${(Number(value) / 1e9).toFixed(1)}B`,
            font: { size: 11 },
            color: '#6b7280'
          }
        },
        y: {
          grid: {
            display: false,
            drawBorder: false
          },
          ticks: {
            font: { size: 11 },
            color: '#374151'
          }
        }
      }
    }
  });
}