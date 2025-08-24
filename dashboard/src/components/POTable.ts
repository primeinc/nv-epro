import type { PurchaseOrder } from '../types';
import { StatusBadge } from './StatusBadge';

export interface POTableProps {
  purchaseOrders: PurchaseOrder[];
  onVendorClick?: (vendorName: string) => void;
  highlightTerm?: string;
}

export function POTable({ purchaseOrders, onVendorClick, highlightTerm }: POTableProps): string {
  if (purchaseOrders.length === 0) {
    return '<p style="text-align: center; color: #666;">No purchase orders found</p>';
  }
  
  return `
    <div style="overflow-x: auto;">
      <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr style="background: #f5f5f5;">
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">PO ID</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Date</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Vendor</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Amount</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Department</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Buyer</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Status</th>
            <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">Description</th>
          </tr>
        </thead>
        <tbody>
          ${purchaseOrders.map(po => `
            <tr>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                <strong>${highlight(po.po_id, highlightTerm)}</strong>
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${new Date(po.sent_date).toLocaleDateString()}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${renderVendorLink(po.vendor_name, onVendorClick, highlightTerm)}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                $${(po.total_amount / 1e3).toFixed(1)}K
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${highlight(po.department || '-', highlightTerm)}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${highlight(po.buyer_name || '-', highlightTerm)}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #eee;">
                ${StatusBadge(po.status_category)}
              </td>
              <td style="
                padding: 12px; 
                border-bottom: 1px solid #eee; 
                max-width: 300px; 
                overflow: hidden; 
                text-overflow: ellipsis; 
                white-space: nowrap;
              " title="${po.description || ''}">
                ${highlight(po.description || '-', highlightTerm)}
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderVendorLink(vendorName: string | undefined, onVendorClick?: (name: string) => void, highlightTerm?: string): string {
  if (!vendorName) return '-';
  
  if (onVendorClick) {
    return `
      <span 
        onclick="window.showVendorDetail('${vendorName.replace(/'/g, "\\'")}')" 
        style="color: #2196F3; cursor: pointer; text-decoration: underline;"
      >
        ${highlight(vendorName, highlightTerm)}
      </span>
    `;
  }
  
  return highlight(vendorName, highlightTerm);
}

function highlight(text: string, searchTerm?: string): string {
  if (!searchTerm || !text) return text;
  
  const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
  return text.replace(regex, '<mark style="background: yellow; padding: 2px;">$1</mark>');
}