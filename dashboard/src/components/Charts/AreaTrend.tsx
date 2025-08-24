import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useMemo } from 'react'
import { useStore } from '../../lib/state'
import dayjs from 'dayjs'
import { formatUSD } from '../../lib/utils'

export default function AreaTrend() {
  const byMonth = useStore(s => s.byMonthTotals)
  const dailyCounts = useStore(s => s.dailyCounts)
  const filtered = useStore(s => s.filtered)
  
  // Decide whether to show daily or monthly based on date range
  const dateRange = useMemo(() => {
    if (filtered.length === 0) return 0
    const dates = filtered.map(po => po.sent_date).sort()
    if (dates.length === 0) return 0
    const min = dayjs(dates[0])
    const max = dayjs(dates[dates.length - 1])
    return max.diff(min, 'days')
  }, [filtered])
  
  // Use daily data for ranges < 90 days, weekly for < 365, monthly for larger
  const useDaily = dateRange <= 90
  const useWeekly = dateRange > 90 && dateRange <= 365
  
  const data = useMemo(() => {
    if (useDaily && dailyCounts.length > 0) {
      // Show daily data
      return dailyCounts.map(d => ({
        date: dayjs(d.date).format('MMM D'),
        value: Math.round(d.total)
      }))
    } else if (useWeekly && dailyCounts.length > 0) {
      // Aggregate to weekly
      const weeks = new Map<string, { total: number; count: number }>()
      dailyCounts.forEach(d => {
        const week = dayjs(d.date).startOf('week').format('YYYY-MM-DD')
        const w = weeks.get(week) ?? { total: 0, count: 0 }
        w.total += d.total
        w.count += d.count
        weeks.set(week, w)
      })
      return Array.from(weeks.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([week, data]) => ({
          date: dayjs(week).format('MMM D'),
          value: Math.round(data.total)
        }))
    } else {
      // Use monthly data
      return byMonth.map(m => ({
        date: dayjs(m.month + '-01').format('MMM YY'),
        value: Math.round(m.total)
      }))
    }
  }, [dailyCounts, byMonth, useDaily, useWeekly])

  // Apply logarithmic-like compression to make outliers visible without crushing normal data
  const compressedData = useMemo(() => {
    return data.map(d => {
      const value = d.value;
      // Apply square root transformation to compress high values
      // This makes outliers visible while keeping normal spending readable
      const compressed = Math.sqrt(value) * 1000; // Scale back up for display
      return {
        ...d,
        originalValue: value,
        value: compressed
      };
    });
  }, [data]);

  const option: EChartsOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const param = params[0];
        if (param && param.data) {
          const originalValue = data[param.dataIndex]?.value;
          return `${param.name}<br/>${formatUSD(originalValue || 0)}`;
        }
        return '';
      }
    },
    grid: { left: 58, right: 12, top: 12, bottom: 40 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: compressedData.map(d => d.date),
      axisLabel: {
        rotate: data.length > 20 ? 45 : 0,
        interval: Math.floor(data.length / 8), // Show max ~8 labels
        fontSize: 11
      }
    },
    yAxis: { 
      type: 'value',
      axisLabel: {
        formatter: (v: number) => {
          // Convert back from compressed scale for display
          const original = (v / 1000) ** 2;
          return original >= 1e9 ? `$${(original/1e9).toFixed(1)}B` : 
                 original >= 1e6 ? `$${(original/1e6).toFixed(0)}M` : 
                 original >= 1e3 ? `$${(original/1e3).toFixed(0)}K` : `$${original.toFixed(0)}`;
        }
      }
    },
    series: [{
      type: 'line',
      data: compressedData.map(d => d.value),
      smooth: true,
      areaStyle: {
        opacity: 0.3
      },
      showSymbol: false,
      lineStyle: {
        width: 2
      }
    }]
  }), [data])

  return <ReactECharts option={option} style={{ width: '100%', height: '100%' }} />
}