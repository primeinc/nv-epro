import Chart from 'chart.js/auto';
import type { 
  DashboardMetrics, 
  VendorSummary, 
  MonthlySummary, 
  DepartmentSummary, 
  StatusSummary 
} from '../types';

export function renderDashboard(
  metrics: DashboardMetrics, 
  vendors: VendorSummary[], 
  monthly: MonthlySummary[], 
  departments: DepartmentSummary[], 
  statuses: StatusSummary[]
) {
  const app = document.getElementById('app')!;
  
  app.innerHTML = `
    <div style="font-family: system-ui, -apple-system, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px;">
      <h1>Nevada Procurement Dashboard</h1>
      
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin-bottom: 40px;">
        ${renderMetricCard('Purchase Orders', (metrics.poCount || 0).toLocaleString(), `$${((metrics.poTotal || 0) / 1e9).toFixed(2)}B total`)}
        ${renderMetricCard('Average PO Size', `$${((metrics.avgPOAmount || 0) / 1e3).toFixed(1)}K`, 'Per order')}
        ${renderMetricCard('Active POs', (metrics.activePOs || 0).toLocaleString(), 'Sent status', '#e8f5e9', '#4caf50')}
        ${renderMetricCard('Completed', (metrics.completedPOs || 0).toLocaleString(), 'Complete/Closed', '#f3e5f5', '#9c27b0')}
        ${renderMetricCard('Contracts', (metrics.contractCount || 0).toLocaleString())}
        ${renderMetricCard('Vendors', (metrics.vendorCount || 0).toLocaleString())}
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
  
  // Render charts after DOM is updated
  setTimeout(() => {
    renderVendorChart(vendors);
    renderMonthlyChart(monthly);
    renderDepartmentChart(departments);
    renderStatusChart(statuses);
  }, 0);
}

function renderMetricCard(
  title: string, 
  value: string, 
  subtitle?: string, 
  bgColor: string = '#f5f5f5', 
  valueColor?: string
): string {
  return `
    <div style="background: ${bgColor}; padding: 20px; border-radius: 8px;">
      <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">${title}</h3>
      <div style="font-size: 24px; font-weight: bold; ${valueColor ? `color: ${valueColor};` : ''}">${value}</div>
      ${subtitle ? `<div style="color: #666; font-size: 14px;">${subtitle}</div>` : ''}
    </div>
  `;
}

function renderVendorChart(vendors: VendorSummary[]) {
  if (vendors.length === 0) return;
  
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
          (window as any).showVendorDetail(vendor.vendor);
        }
      },
      onHover: (event, activeElements) => {
        (event.native?.target as HTMLElement).style.cursor = activeElements.length > 0 ? 'pointer' : 'default';
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

function renderMonthlyChart(monthly: MonthlySummary[]) {
  if (monthly.length === 0) return;
  
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

function renderDepartmentChart(departments: DepartmentSummary[]) {
  if (departments.length === 0) return;
  
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

function renderStatusChart(statuses: StatusSummary[]) {
  if (statuses.length === 0) return;
  
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