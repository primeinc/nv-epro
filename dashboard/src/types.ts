export interface ManifestData {
  parquet: string[];
}

export interface VendorSummary {
  vendor: string;
  total: number;
  count: number;
}

export interface MonthlySummary {
  month: string;
  total: number;
  count: number;
}

export interface DepartmentSummary {
  department: string;
  total: number;
  count: number;
}

export interface StatusSummary {
  status: string;
  count: number;
}

export interface DashboardMetrics {
  poCount?: number;
  poTotal?: number;
  avgPOAmount?: number;
  activePOs?: number;
  completedPOs?: number;
  contractCount?: number;
  vendorCount?: number;
}

export interface PurchaseOrder {
  po_id: string;
  sent_date: string;
  total_amount: number;
  department: string | null;
  buyer_name: string | null;
  status_category: string;
  description: string | null;
}

export interface VendorDetail {
  vendor_name: string;
  po_count: number;
  total_amount: number;
  avg_amount: number;
  first_po: string;
  last_po: string;
}