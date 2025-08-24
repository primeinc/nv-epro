export type POStatus = 'Sent' | 'Complete' | 'Closed' | 'Partial';

export interface PurchaseOrder {
  po_id: string;
  vendor_name: string;
  sent_date: string;          // ISO date
  total_amount: number;       // USD
  department: string | null;
  buyer_name: string | null;
  status_category: POStatus;
  description: string | null;
}

export interface DashboardMetrics {
  poCount: number;
  poTotal: number;
  avgPOAmount: number;
  activePOs: number;
  completedPOs: number;
  vendorCount: number;
}

export interface VendorSummary {
  vendor: string;
  total: number;
  count: number;
  avg: number;
  first: string;
  last: string;
}

export interface DateRange {
  from: string | null; // ISO
  to: string | null;   // ISO
}