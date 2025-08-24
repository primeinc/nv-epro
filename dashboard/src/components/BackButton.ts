export interface BackButtonProps {
  text?: string;
  onClick?: () => void;
}

export function BackButton({ text = '← Back to Dashboard', onClick }: BackButtonProps = {}): string {
  const clickHandler = onClick ? 
    `(() => { ${onClick.toString()}(); })()` : 
    'window.showDashboard()';
  
  return `
    <button 
      onclick="${clickHandler}" 
      style="
        padding: 8px 16px; 
        background: #2196F3; 
        color: white; 
        border: none; 
        border-radius: 4px; 
        cursor: pointer;
        font-size: 14px;
        transition: background 0.2s;
      "
      onmouseover="this.style.background='#1976D2'"
      onmouseout="this.style.background='#2196F3'"
    >
      ${text}
    </button>
  `;
}