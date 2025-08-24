import type { VendorSummary } from '../types';

export interface VendorCardProps {
  vendor: VendorSummary;
  onClick?: (vendorName: string) => void;
}

export function VendorCard({ vendor, onClick }: VendorCardProps): string {
  const clickHandler = onClick ? 
    `(() => { ${onClick.toString()}('${vendor.vendor.replace(/'/g, "\\'")}'); })()` :
    `window.showVendorDetail('${vendor.vendor.replace(/'/g, "\\'")}')`;
  
  return `
    <div 
      onclick="${clickHandler}" 
      style="
        background: #f5f5f5; 
        padding: 15px; 
        border-radius: 8px; 
        cursor: pointer; 
        transition: all 0.2s;
        border: 1px solid transparent;
      "
      onmouseover="this.style.background='#e0e0e0'; this.style.borderColor='#2196F3';" 
      onmouseout="this.style.background='#f5f5f5'; this.style.borderColor='transparent';"
    >
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
          <div style="font-weight: bold; font-size: 16px; color: #333;">
            ${vendor.vendor}
          </div>
          <div style="color: #666; font-size: 14px; margin-top: 4px;">
            ${vendor.count.toLocaleString()} Purchase Orders
          </div>
        </div>
        <div style="text-align: right;">
          <div style="font-weight: bold; color: #2196F3; font-size: 18px;">
            $${(vendor.total / 1e6).toFixed(2)}M
          </div>
          <div style="color: #999; font-size: 12px;">
            Total Amount
          </div>
        </div>
      </div>
    </div>
  `;
}