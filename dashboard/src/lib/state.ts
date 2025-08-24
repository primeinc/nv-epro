import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import dayjs from 'dayjs';
import type { PurchaseOrder, DateRange, DashboardMetrics, VendorSummary, POStatus } from './types';
import { inDateRange, matchesQuery, monthKey } from './utils';

export interface Filters {
  query: string;
  status: POStatus | 'All';
  department: string | 'All';
  vendor: string | 'All';
  date: DateRange;
}

export interface StoreState {
  allPOs: PurchaseOrder[];
  filtered: PurchaseOrder[];
  filters: Filters;
  departments: string[];
  vendors: string[];
  statuses: POStatus[];
  setPOs: (pos: PurchaseOrder[]) => void;
  setFilters: (partial: Partial<Filters>) => void;
  resetFilters: () => void;
  metrics: DashboardMetrics;
  vendorSummaries: VendorSummary[];
  byMonthTotals: { month: string; total: number; count: number }[];
  statusCounts: { status: string; count: number }[];
  dailyCounts: { date: string; count: number; total: number }[];
}

const initialFilters: Filters = {
  query: '',
  status: 'All',
  department: 'All',
  vendor: 'All',
  date: { from: null, to: null }, // No date filter by default - show all data
};

function compute(state: Omit<StoreState, 'filtered' | 'metrics' | 'vendorSummaries' | 'byMonthTotals' | 'statusCounts' | 'dailyCounts' | 'setPOs' | 'setFilters' | 'resetFilters'>): Pick<StoreState, 'filtered' | 'metrics' | 'vendorSummaries' | 'byMonthTotals' | 'statusCounts' | 'dailyCounts'> {
  const { allPOs, filters } = state;
  const f = allPOs.filter(po => {
    // Only apply date filter if dates are set
    if (filters.date.from || filters.date.to) {
      if (!inDateRange(po.sent_date, filters.date.from, filters.date.to)) return false;
    }
    if (filters.status !== 'All' && po.status_category !== filters.status) return false;
    if (filters.department !== 'All' && (po.department ?? '') !== filters.department) return false;
    if (filters.vendor !== 'All' && po.vendor_name !== filters.vendor) return false;
    if (!matchesQuery(po.po_id, filters.query) &&
        !matchesQuery(po.vendor_name, filters.query) &&
        !matchesQuery(po.department ?? '', filters.query) &&
        !matchesQuery(po.buyer_name ?? '', filters.query) &&
        !matchesQuery(po.description ?? '', filters.query)) return false;
    return true;
  });

  const vendorMap = new Map<string, { total: number; count: number; first: string; last: string }>();
  const monthMap = new Map<string, { total: number; count: number }>();
  const statusMap = new Map<string, number>();
  const dayMap = new Map<string, { count: number; total: number }>();

  for (const po of f) {
    const v = vendorMap.get(po.vendor_name) ?? { total: 0, count: 0, first: po.sent_date, last: po.sent_date };
    v.total += po.total_amount; v.count += 1;
    if (po.sent_date < v.first) v.first = po.sent_date;
    if (po.sent_date > v.last) v.last = po.sent_date;
    vendorMap.set(po.vendor_name, v);

    const mk = monthKey(po.sent_date);
    const mm = monthMap.get(mk) ?? { total: 0, count: 0 }; mm.total += po.total_amount; mm.count += 1; monthMap.set(mk, mm);

    statusMap.set(po.status_category, (statusMap.get(po.status_category) ?? 0) + 1);

    const day = po.sent_date.slice(0, 10);
    const dm = dayMap.get(day) ?? { count: 0, total: 0 }; dm.count += 1; dm.total += po.total_amount; dayMap.set(day, dm);
  }

  const vendorSummaries = Array.from(vendorMap.entries()).map(([vendor, v]) => ({
    vendor,
    total: v.total,
    count: v.count,
    avg: v.total / v.count,
    first: v.first,
    last: v.last,
  })).sort((a,b) => b.total - a.total);

  const byMonthTotals = Array.from(monthMap.entries()).map(([month, v]) => ({ month, total: v.total, count: v.count }))
    .sort((a,b) => a.month.localeCompare(b.month));

  const statusCounts = Array.from(statusMap.entries()).map(([status, count]) => ({ status, count }));

  const dailyCounts = Array.from(dayMap.entries()).map(([date, v]) => ({ date, count: v.count, total: v.total }))
    .sort((a,b) => a.date.localeCompare(b.date));

  const metrics = {
    poCount: f.length,
    poTotal: f.reduce((s, x) => s + x.total_amount, 0),
    avgPOAmount: f.length ? f.reduce((s, x) => s + x.total_amount, 0) / f.length : 0,
    activePOs: f.filter(x => x.status_category === 'Sent' || x.status_category === 'Partial').length,
    completedPOs: f.filter(x => x.status_category === 'Complete' || x.status_category === 'Closed').length,
    vendorCount: vendorSummaries.length,
  };

  return { filtered: f, metrics, vendorSummaries, byMonthTotals, statusCounts, dailyCounts };
}

export const useStore = create<StoreState>()(persist((set, get) => ({
  allPOs: [],
  filtered: [],
  filters: initialFilters,
  departments: [],
  vendors: [],
  statuses: ['Sent','Complete','Closed','Partial'],
  setPOs: (pos) => set((state) => {
    const departments = Array.from(new Set(pos.map(p => p.department ?? '').filter(Boolean))).sort();
    const vendors = Array.from(new Set(pos.map(p => p.vendor_name))).sort();
    const next = { ...state, allPOs: pos, departments, vendors };
    return { ...next, ...compute(next) };
  }),
  setFilters: (partial) => set((state) => {
    const filters = { ...state.filters, ...partial };
    const next = { ...state, filters };
    return { ...next, ...compute(next) };
  }),
  resetFilters: () => set((state) => {
    const filters = initialFilters;
    const next = { ...state, filters };
    return { ...next, ...compute(next) };
  }),
  metrics: { poCount: 0, poTotal: 0, avgPOAmount: 0, activePOs: 0, completedPOs: 0, vendorCount: 0 },
  vendorSummaries: [],
  byMonthTotals: [],
  statusCounts: [],
  dailyCounts: [],
}), { 
  name: 'csg-dashboard',
  partialize: (state) => ({ filters: state.filters }) // Only persist filters, not data
}));