import * as React from 'react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable, getSortedRowModel, SortingState } from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import StatusBadge from '../Widgets/StatusBadge'
import { PurchaseOrder } from '../../lib/types'
import { formatUSD } from '../../lib/utils'

export default function DataTable({ data }: { data: PurchaseOrder[] }) {
  const [sorting, setSorting] = React.useState<SortingState>([{ id: 'sent_date', desc: true }])

  const columns = React.useMemo<ColumnDef<PurchaseOrder>[]>(() => [
    { id: 'po_id', accessorKey: 'po_id', header: 'PO', cell: info => <span className="po-id">{info.getValue<string>()}</span> },
    { id: 'vendor_name', accessorKey: 'vendor_name', header: 'Vendor' },
    { id: 'sent_date', accessorKey: 'sent_date', header: 'Date', cell: info => <span className="po-date">{info.getValue<string>().slice(0,10)}</span> },
    { id: 'department', accessorKey: 'department', header: 'Dept' },
    { id: 'buyer_name', accessorKey: 'buyer_name', header: 'Buyer' },
    { id: 'status_category', accessorKey: 'status_category', header: 'Status', cell: info => <div className="po-status"><StatusBadge status={String(info.getValue())} /></div> },
    { id: 'total_amount', accessorKey: 'total_amount', header: 'Amount', cell: info => <div className="po-amount">{formatUSD(info.getValue<number>())}</div> },
    { id: 'description', accessorKey: 'description', header: 'Description' },
  ], [])

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel()
  })

  const parentRef = React.useRef<HTMLDivElement>(null)
  const rowVirtualizer = useVirtualizer({
    count: table.getRowModel().rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 44
  })

  const items = rowVirtualizer.getVirtualItems()
  const totalSize = rowVirtualizer.getTotalSize()

  return (
    <div className="table-wrap">
      <div className="table-scroll" ref={parentRef}>
        <table className="table">
          <thead>
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(h => (
                  <th key={h.id} onClick={h.column.getToggleSortingHandler()} style={{cursor:'pointer'}}>
                    {flexRender(h.column.columnDef.header, h.getContext())}
                    {{ asc: ' ▲', desc: ' ▼' }[h.column.getIsSorted() as string] ?? ''}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody style={{ position: 'relative' }}>
            <tr>
              <td style={{ height: totalSize, position: 'relative', padding: 0 }} colSpan={columns.length}>
                <div style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${items[0]?.start ?? 0}px)` }}>
                  {items.map(vi => {
                    const row = table.getRowModel().rows[vi.index]
                    return (
                      <div key={row.id} data-index={vi.index} style={{ display: 'table', width: '100%', tableLayout: 'fixed', height: vi.size }}>
                        <div style={{ display: 'table-row' }}>
                          {row.getVisibleCells().map(cell => (
                            <div key={cell.id} style={{ display: 'table-cell', borderBottom: '1px solid var(--border)', padding: '10px 14px', verticalAlign: 'middle' }}>
                              {flexRender(cell.column.columnDef.cell ?? cell.column.columnDef.header, cell.getContext())}
                            </div>
                          ))}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}