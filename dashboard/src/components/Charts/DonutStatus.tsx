import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useStore } from '../../lib/state'
import { useMemo } from 'react'

export default function DonutStatus() {
  const statusCounts = useStore(s => s.statusCounts)
  const option: EChartsOption = useMemo(() => ({
    tooltip: { trigger: 'item' },
    series: [{
      type: 'pie',
      radius: ['40%','70%'],
      data: statusCounts.map(s => ({ name: s.status, value: s.count })),
      label: { formatter: '{b}: {c}' }
    }]
  }), [statusCounts])
  return <ReactECharts option={option} style={{ width: '100%', height: 260 }} />
}