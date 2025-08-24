import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useStore } from '../../lib/state'
import { useMemo } from 'react'

export default function CalendarHeatmap() {
  const data = useStore(s => s.dailyCounts)
  const years = Array.from(new Set(data.map(d => d.date.slice(0,4)))).sort()
  const year = years.at(-1) ?? new Date().getFullYear().toString()

  const opt: EChartsOption = useMemo(() => ({
    tooltip: { position: 'top' },
    visualMap: {
      min: 0, max: Math.max(1, Math.max(...data.map(d => d.count))),
      calculable: true, orient: 'horizontal', left: 'center', bottom: 0
    },
    calendar: { range: year, top: 20, left: 24, right: 24, bottom: 40, cellSize: ['auto', 16] },
    series: [{
      type: 'heatmap',
      coordinateSystem: 'calendar',
      data: data.map(d => [d.date, d.count])
    }]
  }), [data, year])

  return <ReactECharts option={opt} style={{ width: '100%', height: 240 }} />
}