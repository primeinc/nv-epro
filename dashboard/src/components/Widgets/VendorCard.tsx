import { useNavigate } from 'react-router-dom'
import { formatUSD } from '../../lib/utils'

export default function VendorCard(props: { vendor: string; total: number; count: number; avg: number; }) {
  const nav = useNavigate()
  return (
    <div className="vendor-card" role="button" onClick={() => nav(`/vendors/${encodeURIComponent(props.vendor)}`)}>
      <div>
        <div className="vendor-name">{props.vendor}</div>
        <div className="vendor-sub">{props.count} POs • avg {formatUSD(props.avg)}</div>
      </div>
      <div className="vendor-total">{formatUSD(props.total)}</div>
    </div>
  )
}