export interface ErrorMessageProps {
  title?: string;
  message: string;
  showBackButton?: boolean;
}

export function ErrorMessage({ 
  title = 'Error', 
  message, 
  showBackButton = true 
}: ErrorMessageProps): string {
  return `
    <div style="
      padding: 40px;
      text-align: center;
      font-family: system-ui, -apple-system, sans-serif;
    ">
      <div style="
        max-width: 600px;
        margin: 0 auto;
        background: #ffebee;
        border: 1px solid #ffcdd2;
        border-radius: 8px;
        padding: 30px;
      ">
        <h1 style="
          color: #c62828;
          margin: 0 0 20px 0;
          font-size: 24px;
        ">
          ${title}
        </h1>
        <p style="
          color: #d32f2f;
          margin: 0 0 20px 0;
          font-size: 16px;
          line-height: 1.5;
        ">
          ${message}
        </p>
        ${showBackButton ? `
          <button 
            onclick="window.showDashboard()" 
            style="
              margin-top: 20px;
              padding: 10px 20px;
              background: #2196F3;
              color: white;
              border: none;
              border-radius: 4px;
              cursor: pointer;
              font-size: 14px;
            "
          >
            Back to Dashboard
          </button>
        ` : ''}
      </div>
    </div>
  `;
}