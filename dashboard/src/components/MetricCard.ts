export interface MetricCardProps {
  title: string;
  value: string;
  subtitle?: string;
  bgColor?: string;
  valueColor?: string;
}

export function MetricCard({ 
  title, 
  value, 
  subtitle, 
  bgColor = '#f5f5f5', 
  valueColor 
}: MetricCardProps): string {
  return `
    <div style="background: ${bgColor}; padding: 20px; border-radius: 8px;">
      <h3 style="margin: 0 0 10px 0; color: #666; font-size: 14px;">${title}</h3>
      <div style="font-size: 24px; font-weight: bold; ${valueColor ? `color: ${valueColor};` : ''}">${value}</div>
      ${subtitle ? `<div style="color: #666; font-size: 14px;">${subtitle}</div>` : ''}
    </div>
  `;
}