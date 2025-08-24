import { useStore } from '../lib/state'
import DataTable from '../components/Table/DataTable'

export default function PurchaseOrders() {
  const pos = useStore(s => s.filtered)

  return (
    <section className="section container stack">
      <h2 style={{margin:0}}>Purchase Orders</h2>
      <DataTable data={pos} />
    </section>
  )
}