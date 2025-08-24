import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'

export default function Sparkline({ data }: { data: number[] }) {
  const option: EChartsOption = {
    grid: { left: 0, right: 0, top: 0, bottom: 0 },
    xAxis: { type: 'category', show: false, data: data.map((_,i) => i) },
    yAxis: { type: 'value', show: false },
    series: [{ type: 'line', data, smooth: true, areaStyle: {}, showSymbol: false }]
  }
  return <ReactECharts option={option} style={{ width: '100%', height: 60 }} />
}