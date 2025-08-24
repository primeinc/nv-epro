import { useStore } from '../lib/state'
import VendorCard from '../components/Widgets/VendorCard'

export default function Vendors() {
  const vendors = useStore(s => s.vendorSummaries)

  return (
    <section className="section container stack">
      <h2 style={{margin:0}}>Vendors</h2>
      <div className="stack">
        <div className="metrics-grid">
          {vendors.slice(0, 24).map(v => (
            <VendorCard key={v.vendor} vendor={v.vendor} total={v.total} count={v.count} avg={v.avg} />
          ))}
        </div>
      </div>
    </section>
  )
}