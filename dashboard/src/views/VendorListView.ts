import { Icons } from '../components/Icons';

export interface VendorListItem {
  vendor: string;
  latestPODate: string;
  totalPOs: number;
  totalAmount: number;
  avgAmount: number;
  latestPOId: string;
  latestPOAmount: number;
  latestPOStatus: string;
  latestDepartment: string;
}

export function renderVendorList(
  vendors: VendorListItem[],
  totalCount: number,
  currentPage: number,
  pageSize: number
): string {
  const totalPages = Math.ceil(totalCount / pageSize);
  
  return `
    <div class="vendor-list-container">
      <div class="section-header">
        <h2 class="section-title">All Vendors</h2>
        <p class="section-subtitle">Showing ${vendors.length} of ${totalCount.toLocaleString()} vendors, sorted by most recent purchase order</p>
      </div>
      
      <div class="vendor-table-container">
        <table class="vendor-table">
          <thead>
            <tr>
              <th>Vendor Name</th>
              <th>Latest PO Date</th>
              <th>Latest PO</th>
              <th>Department</th>
              <th>Total POs</th>
              <th>Total Value</th>
              <th>Avg Value</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            ${vendors.map(vendor => `
              <tr class="vendor-row" onclick="window.showVendorDetail('${vendor.vendor.replace(/'/g, "\\'")}')">
                <td class="vendor-name">
                  <div class="vendor-name-cell">
                    ${vendor.vendor}
                  </div>
                </td>
                <td class="po-date">
                  ${vendor.latestPODate ? new Date(vendor.latestPODate).toLocaleDateString('en-US', {
                    year: 'numeric',
                    month: 'short',
                    day: 'numeric'
                  }) : '—'}
                </td>
                <td class="po-number">
                  ${vendor.latestPOId || '—'}
                </td>
                <td class="department">
                  ${vendor.latestDepartment || '—'}
                </td>
                <td class="po-count">
                  ${vendor.totalPOs.toLocaleString()}
                </td>
                <td class="total-amount">
                  $${(vendor.totalAmount / 1e6).toFixed(2)}M
                </td>
                <td class="avg-amount">
                  $${(vendor.avgAmount / 1e3).toFixed(1)}K
                </td>
                <td>
                  <span class="status-badge status-${(vendor.latestPOStatus || 'unknown').toLowerCase()}">
                    ${vendor.latestPOStatus || '—'}
                  </span>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
      
      <!-- Pagination -->
      <div class="pagination">
        <button 
          class="pagination-btn" 
          onclick="window.loadVendorPage(${currentPage - 1})"
          ${currentPage === 1 ? 'disabled' : ''}
        >
          Previous
        </button>
        <span class="pagination-info">
          Page ${currentPage} of ${totalPages}
        </span>
        <button 
          class="pagination-btn" 
          onclick="window.loadVendorPage(${currentPage + 1})"
          ${currentPage === totalPages ? 'disabled' : ''}
        >
          Next
        </button>
      </div>
    </div>
  `;
}