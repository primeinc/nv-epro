import type { SearchResults } from '../db/search-queries';

export function renderSearchResults(results: SearchResults, searchTerm: string) {
  const app = document.getElementById('app')!;
  
  app.innerHTML = `
    <div style="font-family: system-ui, -apple-system, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px;">
      <div style="margin-bottom: 20px;">
        <button onclick="window.showDashboard()" style="padding: 8px 16px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer;">
          ← Back to Dashboard
        </button>
      </div>
      
      <h1>Search Results for "${searchTerm}"</h1>
      <p style="color: #666;">Found ${results.totalResults} results</p>
      
      ${results.vendors.length > 0 ? `
        <div style="margin-bottom: 40px;">
          <h2>Vendors (${results.vendors.length})</h2>
          <div style="display: grid; gap: 10px;">
            ${results.vendors.map(vendor => `
              <div 
                onclick="window.showVendorDetail('${vendor.vendor.replace(/'/g, "\\'")}')" 
                style="background: #f5f5f5; padding: 15px; border-radius: 8px; cursor: pointer; transition: background 0.2s;"
                onmouseover="this.style.background='#e0e0e0'" 
                onmouseout="this.style.background='#f5f5f5'"
              >
                <div style="display: flex; justify-content: space-between; align-items: center;">
                  <div>
                    <div style="font-weight: bold; font-size: 16px;">${vendor.vendor}</div>
                    <div style="color: #666; font-size: 14px;">${vendor.count} POs</div>
                  </div>
                  <div style="text-align: right;">
                    <div style="font-weight: bold; color: #2196F3;">$${(vendor.total / 1e6).toFixed(2)}M</div>
                  </div>
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      ` : ''}
      
      ${results.purchaseOrders.length > 0 ? `
        <div>
          <h2>Purchase Orders (${results.purchaseOrders.length})</h2>
          <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse;">
              <thead>
                <tr style="background: #f5f5f5;">
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">PO ID</th>
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Date</th>
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Vendor</th>
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Amount</th>
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Department</th>
                  <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Status</th>
                </tr>
              </thead>
              <tbody>
                ${results.purchaseOrders.map(po => `
                  <tr>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      <strong>${highlightTerm(po.po_id, searchTerm)}</strong>
                    </td>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      ${new Date(po.sent_date).toLocaleDateString()}
                    </td>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      ${po.vendor_name ? `
                        <span 
                          onclick="window.showVendorDetail('${po.vendor_name.replace(/'/g, "\\'")}')" 
                          style="color: #2196F3; cursor: pointer; text-decoration: underline;"
                        >
                          ${highlightTerm(po.vendor_name, searchTerm)}
                        </span>
                      ` : '-'}
                    </td>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      $${(po.total_amount / 1e3).toFixed(1)}K
                    </td>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      ${po.department ? highlightTerm(po.department, searchTerm) : '-'}
                    </td>
                    <td style="padding: 12px; border-bottom: 1px solid #eee;">
                      ${renderStatusBadge(po.status_category)}
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      ` : ''}
      
      ${results.totalResults === 0 ? `
        <div style="text-align: center; padding: 40px; color: #666;">
          <h3>No results found</h3>
          <p>Try adjusting your search terms or filters</p>
        </div>
      ` : ''}
    </div>
  `;
}

function highlightTerm(text: string, searchTerm: string): string {
  if (!searchTerm || !text) return text;
  
  const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
  return text.replace(regex, '<mark style="background: yellow; padding: 2px;">$1</mark>');
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