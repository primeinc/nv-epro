export default function StatusBadge({ status }: { status: string }) {
  return <span className="status-badge" data-status={status}><span className="dot" style={{width:8,height:8,borderRadius:999,background:'currentColor'}}></span>{status}</span>
}