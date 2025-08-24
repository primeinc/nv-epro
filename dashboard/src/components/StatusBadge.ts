export function StatusBadge(status: string): string {
  const styles: Record<string, string> = {
    'Sent': 'background: #e8f5e9; color: #4caf50',
    'Complete': 'background: #e3f2fd; color: #2196f3',
    'Closed': 'background: #f3e5f5; color: #9c27b0',
    'Partial': 'background: #fff3e0; color: #ff9800'
  };
  
  const style = styles[status] || 'background: #f5f5f5; color: #666';
  
  return `
    <span style="
      padding: 2px 8px; 
      border-radius: 12px; 
      font-size: 12px; 
      ${style};
      display: inline-block;
    ">
      ${status}
    </span>
  `;
}