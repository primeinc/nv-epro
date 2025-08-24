import Chart from 'chart.js/auto';
import type { VendorDetail, PurchaseOrder, DepartmentSummary, MonthlySummary } from '../types';

export function renderVendorDetail(
  summary: VendorDetail,
  recentPOs: PurchaseOrder[],
  departments: DepartmentSummary[],
  monthlyData: MonthlySummary[]
) {
  const app = document.getElementById('app')!;
  
  app.innerHTML = `
    <div style="font-family: system-ui, -apple-system, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px;">
      <div style="margin-bottom: 20px;">
        <button onclick="window.showDashboard()" style="padding: 8px 16px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer;">
          ← Back to Dashboard
        </button>
      </div>
      
      <h1>${summary.vendor_name}</h1>
      
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 40px;">
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Total POs</h3>
          <div style="font-size: 24px; font-weight: bold;">${summary.po_count.toLocaleString()}</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Total Amount</h3>
          <div style="font-size: 24px; font-weight: bold;">$${(summary.total_amount / 1e6).toFixed(2)}M</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Average PO</h3>
          <div style="font-size: 24px; font-weight: bold;">$${(summary.avg_amount / 1e3).toFixed(1)}K</div>
        </div>
        
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
          <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Active Since</h3>
          <div style="font-size: 18px; font-weight: bold;">${new Date(summary.first_po).toLocaleDateString()}</div>
          <div style="color: #666; font-size: 14px;">to ${new Date(summary.last_po).toLocaleDateString()}</div>
        </div>
      </div>
      
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 40px; margin-bottom: 40px;">
        <div>
          <h2>Monthly Trend</h2>
          <canvas id="vendorMonthlyChart"></canvas>
        </div>
        
        <div>
          <h2>Departments Served</h2>
          <canvas id="vendorDeptChart"></canvas>
        </div>
      </div>
      
      <div>
        <h2>Recent Purchase Orders</h2>
        ${renderPOTable(recentPOs)}
      </div>
    </div>
  `;
  
  // Render charts after DOM is updated
  setTimeout(() => {
    renderVendorMonthlyChart(monthlyData);
    renderVendorDeptChart(departments);
  }, 100);
}

function renderPOTable(recentPOs: PurchaseOrder[]): string {
  return `
    <div style="overflow-x: auto;">
      <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr style="background: #f5f5f5;">
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">PO ID</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Date</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Amount</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Department</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Buyer</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Status</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Description</th>
          </tr>
        </thead>
        <tbody>
          ${recentPOs.map(po => `
            <tr>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">${po.po_id}</td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">${new Date(po.sent_date).toLocaleDateString()}</td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">$${(po.total_amount / 1e3).toFixed(1)}K</td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">${po.department || '-'}</td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">${po.buyer_name || '-'}</td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${renderStatusBadge(po.status_category)}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${po.description || ''}">${po.description || '-'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderStatusBadge(status: string): string {
  const colors = {
    'Sent': '#e8f5e9; color: #4caf50',
    'Complete': '#e3f2fd; color: #2196f3',
    'Closed': '#f3e5f5; color: #9c27b0',
  };
  
  const style = colors[status as keyof typeof colors] || '#f5f5f5; color: #666';
  
  return `<span style="padding: 2px 8px; border-radius: 12px; font-size: 12px; background: ${style};">${status}</span>`;
}

function renderVendorMonthlyChart(monthlyData: MonthlySummary[]) {
  if (monthlyData.length === 0) return;
  
  new Chart(document.getElementById('vendorMonthlyChart') as HTMLCanvasElement, {
    type: 'line',
    data: {
      labels: monthlyData.reverse().map(m => m.month),
      datasets: [{
        label: 'Total Amount ($)',
        data: monthlyData.map(m => m.total),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false }
      },
      scales: {
        y: {
          ticks: {
            callback: (value) => `$${(Number(value) / 1e6).toFixed(1)}M`
          }
        }
      }
    }
  });
}

function renderVendorDeptChart(departments: DepartmentSummary[]) {
  if (departments.length === 0) return;
  
  new Chart(document.getElementById('vendorDeptChart') as HTMLCanvasElement, {
    type: 'doughnut',
    data: {
      labels: departments.map(d => d.department.length > 20 ? d.department.substring(0, 20) + '...' : d.department),
      datasets: [{
        data: departments.map(d => d.total),
        backgroundColor: [
          'rgba(255, 99, 132, 0.7)',
          'rgba(54, 162, 235, 0.7)',
          'rgba(255, 206, 86, 0.7)',
          'rgba(75, 192, 192, 0.7)',
          'rgba(153, 102, 255, 0.7)',
          'rgba(255, 159, 64, 0.7)',
          'rgba(199, 199, 199, 0.7)',
          'rgba(83, 102, 255, 0.7)',
          'rgba(255, 99, 255, 0.7)',
          'rgba(99, 255, 132, 0.7)'
        ]
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { position: 'right' },
        tooltip: {
          callbacks: {
            label: (context) => {
              const total = departments.reduce((sum, d) => sum + d.total, 0);
              const percent = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: $${(context.parsed / 1e6).toFixed(2)}M (${percent}%)`;
            }
          }
        }
      }
    }
  });
}