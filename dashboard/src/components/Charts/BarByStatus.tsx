import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useMemo } from 'react'
import { useStore } from '../../lib/state'

export default function BarByStatus() {
  const statusCounts = useStore(s => s.statusCounts)
  const option: EChartsOption = useMemo(() => ({
    tooltip: { trigger: 'item' },
    grid: { left: 24, right: 12, top: 20, bottom: 24 },
    xAxis: { type: 'category', data: statusCounts.map(s => s.status) },
    yAxis: { type: 'value' },
    series: [{ type: 'bar', data: statusCounts.map(s => s.count) }]
  }), [statusCounts])
  return <ReactECharts option={option} style={{ width: '100%', height: 280 }} />
}