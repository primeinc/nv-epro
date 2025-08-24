import type { StatusSummary } from '../types';

export function StatusMiniCard(statuses: StatusSummary[]): string {
  if (statuses.length === 0) return '';
  
  // Calculate percentages
  const total = statuses.reduce((sum, s) => sum + s.count, 0);
  const statusMap = new Map(statuses.map(s => [s.status, s.count]));
  
  const sent = statusMap.get('Sent') || 0;
  const complete = statusMap.get('Complete') || 0;
  const closed = statusMap.get('Closed') || 0;
  const partial = statusMap.get('Partial') || 0;
  
  const sentPct = ((sent / total) * 100).toFixed(0);
  const completePct = ((complete / total) * 100).toFixed(0);
  const closedPct = ((closed / total) * 100).toFixed(0);
  const partialPct = ((partial / total) * 100).toFixed(0);
  
  return `
    <div style="background: #f5f5f5; padding: 20px; border-radius: 8px;">
      <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">Status Distribution</h3>
      <div style="display: flex; gap: 2px; height: 20px; border-radius: 4px; overflow: hidden; margin-bottom: 10px;">
        ${sent > 0 ? `<div style="background: #4caf50; flex: ${sent};" title="Sent: ${sentPct}%"></div>` : ''}
        ${complete > 0 ? `<div style="background: #2196f3; flex: ${complete};" title="Complete: ${completePct}%"></div>` : ''}
        ${closed > 0 ? `<div style="background: #9c27b0; flex: ${closed};" title="Closed: ${closedPct}%"></div>` : ''}
        ${partial > 0 ? `<div style="background: #ff9800; flex: ${partial};" title="Partial: ${partialPct}%"></div>` : ''}
      </div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 4px; font-size: 11px;">
        ${sent > 0 ? `<div><span style="display: inline-block; width: 8px; height: 8px; background: #4caf50; border-radius: 2px;"></span> Sent ${sentPct}%</div>` : ''}
        ${complete > 0 ? `<div><span style="display: inline-block; width: 8px; height: 8px; background: #2196f3; border-radius: 2px;"></span> Complete ${completePct}%</div>` : ''}
        ${closed > 0 ? `<div><span style="display: inline-block; width: 8px; height: 8px; background: #9c27b0; border-radius: 2px;"></span> Closed ${closedPct}%</div>` : ''}
        ${partial > 0 ? `<div><span style="display: inline-block; width: 8px; height: 8px; background: #ff9800; border-radius: 2px;"></span> Partial ${partialPct}%</div>` : ''}
      </div>
    </div>
  `;
}