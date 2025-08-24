import Chart from 'chart.js/auto';
import type { 
  DashboardMetrics, 
  VendorSummary, 
  MonthlySummary, 
  DepartmentSummary, 
  StatusSummary 
} from '../types';
import { Icons } from '../components/Icons';

export function renderDashboard(
  metrics: DashboardMetrics, 
  vendors: VendorSummary[], 
  monthly: MonthlySummary[], 
  departments: DepartmentSummary[], 
  statuses: StatusSummary[],
  searchBar: string = ''
) {
  const app = document.getElementById('app')!;
  
  app.innerHTML = `
    <div class="dashboard-header">
      <div class="dashboard-header-content">
        <h1 class="dashboard-title">Nevada Procurement Dashboard</h1>
        ${searchBar}
      </div>
    </div>
    <div class="dashboard-container">
      
      <!-- Dataset Overview Section -->
      <div class="section-header">
        <h2 class="section-title">Dataset Overview</h2>
        <p class="section-subtitle">Four integrated procurement datasets from Nevada ePro</p>
      </div>
      
      <div class="dataset-grid">
        <div class="dataset-card">
          <div class="dataset-card-header">
            <div class="dataset-icon" style="background: rgba(59, 130, 246, 0.1); color: #3B82F6;">
              ${Icons.package}
            </div>
            <div class="dataset-label">Purchase Orders</div>
          </div>
          <div class="dataset-value">${(metrics.poCount || 0).toLocaleString()}</div>
          <div class="dataset-subtitle">Total Records</div>
          <div class="dataset-meta">
            <span class="meta-item">$${((metrics.poTotal || 0) / 1e9).toFixed(2)}B value</span>
          </div>
        </div>
        
        <div class="dataset-card">
          <div class="dataset-card-header">
            <div class="dataset-icon" style="background: rgba(139, 92, 246, 0.1); color: #8B5CF6;">
              ${Icons.file}
            </div>
            <div class="dataset-label">Contracts</div>
          </div>
          <div class="dataset-value">${(metrics.contractCount || 0).toLocaleString()}</div>
          <div class="dataset-subtitle">Total Records</div>
          <div class="dataset-meta">
            <span class="meta-item">Active agreements</span>
          </div>
        </div>
        
        <div class="dataset-card">
          <div class="dataset-card-header">
            <div class="dataset-icon" style="background: rgba(16, 185, 129, 0.1); color: #10B981;">
              ${Icons.trending}
            </div>
            <div class="dataset-label">Bids</div>
          </div>
          <div class="dataset-value">—</div>
          <div class="dataset-subtitle">Coming Soon</div>
          <div class="dataset-meta">
            <span class="meta-item">Data pending</span>
          </div>
        </div>
        
        <div class="dataset-card">
          <div class="dataset-card-header">
            <div class="dataset-icon" style="background: rgba(245, 158, 11, 0.1); color: #F59E0B;">
              ${Icons.building}
            </div>
            <div class="dataset-label">Vendors</div>
          </div>
          <div class="dataset-value">${(metrics.vendorCount || 0).toLocaleString()}</div>
          <div class="dataset-subtitle">Unique Vendors</div>
          <div class="dataset-meta">
            <span class="meta-item">Across all datasets</span>
          </div>
        </div>
      </div>
      
      <!-- Purchase Orders Section -->
      <div class="section-divider"></div>
      
      <div class="section-header">
        <h2 class="section-title">Purchase Order Analytics</h2>
        <p class="section-subtitle">Detailed analysis of ${(metrics.poCount || 0).toLocaleString()} purchase orders</p>
      </div>
      
      <!-- PO Metrics -->
      <div class="po-metrics-grid">
        <div class="metric-card">
          <div class="metric-card-title">Average PO Value</div>
          <div class="metric-card-value">$${((metrics.avgPOAmount || 0) / 1e3).toFixed(1)}K</div>
          <div class="metric-card-subtitle">Per order</div>
        </div>
        
        <div class="metric-card">
          <div class="metric-card-title">Active Orders</div>
          <div class="metric-card-value">${(metrics.activePOs || 0).toLocaleString()}</div>
          <div class="metric-card-subtitle">Status: Sent</div>
        </div>
        
        <div class="metric-card">
          <div class="metric-card-title">Completed Orders</div>
          <div class="metric-card-value">${(metrics.completedPOs || 0).toLocaleString()}</div>
          <div class="metric-card-subtitle">Complete/Closed</div>
        </div>
        
        <div class="metric-card po-status-card">
          <div class="metric-card-title">Order Status Breakdown</div>
          <div class="status-distribution">
            ${statuses.map(s => {
              const total = statuses.reduce((sum, st) => sum + st.count, 0);
              const percent = Math.round(s.count / total * 100);
              const colors: Record<string, string> = {
                'Sent': '#10B981',
                'Complete': '#3B82F6',
                'Closed': '#8B5CF6',
                'Partial': '#F59E0B'
              };
              return `
                <div class="status-bar">
                  <div class="status-bar-header">
                    <span class="status-name">${s.status}</span>
                    <span class="status-value">${s.count.toLocaleString()} (${percent}%)</span>
                  </div>
                  <div class="status-bar-track">
                    <div class="status-bar-fill" style="width: ${percent}%; background: ${colors[s.status] || '#94A3B8'};"></div>
                  </div>
                </div>
              `;
            }).join('')}
          </div>
        </div>
      </div>
      
      <!-- PO Charts Row -->
      <div class="charts-row">
        <div class="chart-card">
          <div class="chart-card-header">
            <h3 class="chart-card-title">Top Vendors by PO Value</h3>
            <button class="icon-btn" title="Export">${Icons.export}</button>
          </div>
          <div style="position: relative; height: 320px; width: 100%;">
            <canvas id="vendorChart"></canvas>
          </div>
        </div>
        
        <div class="chart-card">
          <div class="chart-card-header">
            <h3 class="chart-card-title">Monthly PO Trend</h3>
            <button class="icon-btn" title="Export">${Icons.export}</button>
          </div>
          <div style="position: relative; height: 320px; width: 100%;">
            <canvas id="monthlyChart"></canvas>
          </div>
        </div>
      </div>
      
      <!-- Departments Chart -->
      <div class="chart-card full-width-card">
        <div class="chart-card-header">
          <h3 class="chart-card-title">Department Spending (Purchase Orders)</h3>
          <button class="icon-btn" title="Export">${Icons.export}</button>
        </div>
        <div style="position: relative; height: 320px; width: 100%;">
          <canvas id="departmentChart"></canvas>
        </div>
      </div>
      
      <!-- Footer -->
      <div class="dashboard-footer">
        <p>Data source: <a href="https://nevadaepro.com/bso/" target="_blank">Nevada ePro</a> | Repository: <a href="https://github.com/primeinc/nevada-procurement-data" target="_blank">github.com/primeinc/nevada-procurement-data</a></p>
        <p style="margin-top: 10px;">Last updated: ${new Date().toLocaleString()}</p>
      </div>
    </div>
  `;
  
  // Render charts after DOM is updated
  setTimeout(() => {
    renderVendorChart(vendors);
    renderMonthlyChart(monthly);
    renderDepartmentChart(departments);
  }, 0);
}

function renderVendorChart(vendors: VendorSummary[]) {
  if (vendors.length === 0) return;
  
  const ctx = (document.getElementById('vendorChart') as HTMLCanvasElement).getContext('2d')!;
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
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const vendor = vendors[index];
          (window as any).showVendorDetail(vendor.vendor);
        }
      },
      onHover: (event, activeElements) => {
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
            label: (context) => `$${(context.parsed.x / 1e6).toFixed(2)}M`
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
            callback: (value) => `$${(Number(value) / 1e6).toFixed(0)}M`,
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

function renderMonthlyChart(monthly: MonthlySummary[]) {
  if (monthly.length === 0) return;
  
  const ctx = document.getElementById('monthlyChart') as HTMLCanvasElement;
  const gradient = ctx.getContext('2d')!.createLinearGradient(0, 0, 0, 400);
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
            label: (context) => `$${(context.parsed.y / 1e6).toFixed(2)}M`
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
            callback: (value) => `$${(Number(value) / 1e6).toFixed(0)}M`,
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

function renderDepartmentChart(departments: DepartmentSummary[]) {
  if (departments.length === 0) return;
  
  const ctx = (document.getElementById('departmentChart') as HTMLCanvasElement).getContext('2d')!;
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
            label: (context) => `$${(context.parsed.x / 1e9).toFixed(2)}B`
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
            callback: (value) => `$${(Number(value) / 1e9).toFixed(1)}B`,
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