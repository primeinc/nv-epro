import type { DashboardMetrics, StatusSummary } from '../types';
import { Icons } from './Icons';

interface MetricCardProps {
  title: string;
  value: string;
  subtitle?: string;
  icon?: string;
  iconColor?: string;
  change?: string;
  changeType?: 'positive' | 'negative';
}

function MetricCard({ title, value, subtitle, icon, iconColor = '#64748B', change, changeType }: MetricCardProps): string {
  return `
    <div class="metric-card">
      ${icon ? `
        <div class="metric-card-icon" style="color: ${iconColor};">
          ${icon}
        </div>
      ` : ''}
      ${change ? `
        <div class="metric-card-change change-${changeType}">
          ${changeType === 'positive' ? Icons.arrowUp : Icons.arrowDown}
          <span>${change}</span>
        </div>
      ` : ''}
      <div class="metric-card-title">${title}</div>
      <div class="metric-card-value">${value}</div>
      ${subtitle ? `<div class="metric-card-subtitle">${subtitle}</div>` : ''}
    </div>
  `;
}

function StatusMiniCard(statuses: StatusSummary[]): string {
  const statusColors: Record<string, string> = {
    'Sent': '#10b981',
    'Complete': '#3B82F6',
    'Closed': '#8B5CF6',
    'Partial': '#F59E0B'
  };
  
  const total = statuses.reduce((sum, s) => sum + s.count, 0);
  
  return `
    <div class="metric-card status-mini-card">
      <div class="metric-card-icon" style="color: #3B82F6;">
        ${Icons.chart}
      </div>
      <div class="metric-card-title">Status Distribution</div>
      <div class="status-distribution">
        ${statuses.map(s => `
          <div class="status-item">
            <div class="status-color" style="background: ${statusColors[s.status] || '#94A3B8'};"></div>
            <div class="status-label">${s.status}</div>
            <div class="status-percent">${Math.round(s.count / total * 100)}%</div>
          </div>
        `).join('')}
      </div>
    </div>
  `;
}

export function MetricsGrid(metrics: DashboardMetrics, statuses: StatusSummary[]): string {
  return `
    <div class="metrics-grid">
      ${MetricCard({
        title: 'Purchase Orders',
        value: (metrics.poCount || 0).toLocaleString(),
        subtitle: `$${((metrics.poTotal || 0) / 1e9).toFixed(2)}B total`,
        icon: Icons.package,
        iconColor: '#3B82F6'
      })}
      
      ${MetricCard({
        title: 'Average PO Size',
        value: `$${((metrics.avgPOAmount || 0) / 1e3).toFixed(1)}K`,
        subtitle: 'Per order',
        icon: Icons.dollar,
        iconColor: '#10B981'
      })}
      
      ${MetricCard({
        title: 'Active POs',
        value: (metrics.activePOs || 0).toLocaleString(),
        subtitle: 'Sent status',
        icon: Icons.trending,
        iconColor: '#0EA5E9',
        change: '12%',
        changeType: 'positive'
      })}
      
      ${MetricCard({
        title: 'Completed',
        value: (metrics.completedPOs || 0).toLocaleString(),
        subtitle: 'Complete/Closed',
        icon: Icons.check,
        iconColor: '#8B5CF6'
      })}
      
      ${MetricCard({
        title: 'Contracts',
        value: (metrics.contractCount || 0).toLocaleString(),
        icon: Icons.file,
        iconColor: '#64748B'
      })}
      
      ${MetricCard({
        title: 'Vendors',
        value: (metrics.vendorCount || 0).toLocaleString(),
        icon: Icons.building,
        iconColor: '#F59E0B'
      })}
      
      ${StatusMiniCard(statuses)}
    </div>
  `;
}