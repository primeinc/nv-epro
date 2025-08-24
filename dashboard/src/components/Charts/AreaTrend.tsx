import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useMemo } from 'react'
import { useStore } from '../../lib/state'
import dayjs from 'dayjs'
import { formatUSD } from '../../lib/utils'

export default function AreaTrend() {
  const byMonth = useStore(s => s.byMonthTotals)

  const option: EChartsOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      valueFormatter: (v) => typeof v === 'number' ? formatUSD(v) : String(v)
    },
    grid: { left: 24, right: 12, top: 20, bottom: 24 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: byMonth.map(m => dayjs(m.month+'-01').format('MMM YYYY'))
    },
    yAxis: { type: 'value' },
    series: [{
      type: 'line',
      data: byMonth.map(m => Math.round(m.total)),
      smooth: true,
      areaStyle: {},
      showSymbol: false
    }]
  }), [byMonth])

  return <ReactECharts option={option} style={{ width: '100%', height: 280 }} />
}