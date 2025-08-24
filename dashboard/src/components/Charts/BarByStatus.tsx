import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useMemo } from 'react'
import { useStore } from '../../lib/state'

export default function BarByStatus() {
  const statusCounts = useStore(s => s.statusCounts)
  
  // Define colors for each status
  const statusColors: Record<string, string> = {
    'Sent': '#6366f1',     // Blue/purple for sent
    'Partial': '#f59e0b',  // Orange for partial
    'Closed': '#10b981',   // Green for closed
    'Cancelled': '#ef4444' // Red for cancelled
  }
  
  const option: EChartsOption = useMemo(() => ({
    tooltip: { trigger: 'item' },
    grid: { left: 24, right: 12, top: 20, bottom: 24 },
    xAxis: { type: 'category', data: statusCounts.map(s => s.status) },
    yAxis: { type: 'value' },
    series: [{ 
      type: 'bar', 
      data: statusCounts.map(s => ({
        value: s.count,
        itemStyle: {
          color: statusColors[s.status] || '#94a3b8' // Default gray if status not found
        }
      }))
    }]
  }), [statusCounts])
  return <ReactECharts option={option} style={{ width: '100%', height: 280 }} />
}