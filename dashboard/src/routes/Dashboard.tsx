import MetricCard from '../components/Widgets/MetricCard'
import AreaTrend from '../components/Charts/AreaTrend'
import BarByStatus from '../components/Charts/BarByStatus'
import DonutStatus from '../components/Charts/DonutStatus'
import CalendarHeatmap from '../components/Charts/CalendarHeatmap'
import { useStore } from '../lib/state'
import { formatUSD } from '../lib/utils'

export default function Dashboard() {
  const m = useStore(s => s.metrics)
  const filtered = useStore(s => s.filtered)

  return (
    <section className="section container stack">
      <div className="metrics-grid">
        <MetricCard title="Purchase Orders" value={m.poCount.toLocaleString()} subtitle="in range" />
        <MetricCard title="Total" value={formatUSD(m.poTotal)} subtitle="sum of PO amounts" />
        <MetricCard title="Average" value={formatUSD(m.avgPOAmount)} subtitle="per PO" />
        <MetricCard title="Vendors" value={m.vendorCount.toLocaleString()} subtitle="with activity" />
      </div>

      <div className="stack">
        <div className="surface" style={{padding:16}}>
          <AreaTrend />
        </div>

        <div className="metrics-grid">
          <div className="surface" style={{padding:16}}><BarByStatus /></div>
          <div className="surface" style={{padding:16}}><DonutStatus /></div>
          <div className="surface" style={{padding:16}}><CalendarHeatmap /></div>
          <div className="surface" style={{padding:24, display:'grid', placeItems:'center', color:'var(--muted)'}}>
            <div>Filtered POs: {filtered.length.toLocaleString()}</div>
          </div>
        </div>
      </div>
    </section>
  )
}