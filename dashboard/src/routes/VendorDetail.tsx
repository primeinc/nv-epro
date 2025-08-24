import { useParams } from 'react-router-dom'
import { useStore } from '../lib/state'
import { formatUSD } from '../lib/utils'
import DonutStatus from '../components/Charts/DonutStatus'
import AreaTrend from '../components/Charts/AreaTrend'
import DataTable from '../components/Table/DataTable'

export default function VendorDetail() {
  const { name } = useParams()
  const decoded = decodeURIComponent(name ?? '')
  const all = useStore(s => s.filtered)
  const vendorPOs = all.filter(p => p.vendor_name === decoded)
  const total = vendorPOs.reduce((s,x)=>s+x.total_amount,0)
  const avg = vendorPOs.length ? total / vendorPOs.length : 0

  return (
    <section className="section container stack">
      <div className="cluster" style={{justifyContent:'space-between'}}>
        <h2 style={{margin:0}}>{decoded}</h2>
        <div className="cluster">
          <div className="badge"><span className="dot" style={{background:'var(--info)'}}></span>{vendorPOs.length} POs</div>
          <div className="badge"><span className="dot" style={{background:'var(--success)'}}></span>{formatUSD(total)}</div>
          <div className="badge"><span className="dot" style={{background:'var(--warning)'}}></span>avg {formatUSD(avg)}</div>
        </div>
      </div>

      <div className="metrics-grid">
        <div className="surface" style={{padding:16}}><AreaTrend /></div>
        <div className="surface" style={{padding:16}}><DonutStatus /></div>
      </div>

      <div className="stack">
        <DataTable data={vendorPOs} />
      </div>
    </section>
  )
}