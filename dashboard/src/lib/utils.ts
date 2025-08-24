import dayjs from 'dayjs';

export function formatUSD(n: number): string {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n);
}

export function safeLower(s: string | null | undefined): string {
  return (s ?? '').toLowerCase();
}

export function matchesQuery(hay: string | null | undefined, q: string): boolean {
  if (!q) return true;
  return safeLower(hay).includes(safeLower(q));
}

export function inDateRange(iso: string, from?: string | null, to?: string | null): boolean {
  const d = dayjs(iso);
  if (from && d.isBefore(from, 'day')) return false;
  if (to && d.isAfter(to, 'day')) return false;
  return true;
}

export function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

export function unique<T>(arr: T[]): T[] {
  return Array.from(new Set(arr));
}

export function monthKey(iso: string): string {
  const d = dayjs(iso);
  return d.format('YYYY-MM');
}

export function formatUSDCompact(n: number): string {
  if (n >= 1e9) {
    return `$${(n / 1e9).toFixed(2)}B`;
  } else if (n >= 1e6) {
    return `$${(n / 1e6).toFixed(2)}M`;
  } else if (n >= 1e3) {
    return `$${(n / 1e3).toFixed(0)}K`;
  }
  return formatUSD(n);
}