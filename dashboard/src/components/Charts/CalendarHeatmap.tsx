import ReactECharts from 'echarts-for-react'
import type { EChartsOption } from 'echarts'
import { useStore } from '../../lib/state'
import { useMemo } from 'react'
import { formatUSDCompact } from '../../lib/utils'

export default function CalendarHeatmap() {
  const dailyCounts = useStore(s => s.dailyCounts)
  const filters = useStore(s => s.filters)
  const filtered = useStore(s => s.filtered)
  
  // Determine which year to show based on the active filter
  const year = useMemo(() => {
    // If a specific year is selected (2018-2024), show that year
    if (filters.date.from && filters.date.to) {
      const fromYear = filters.date.from.slice(0, 4)
      const toYear = filters.date.to.slice(0, 4)
      // If it's a single year filter (like "2023"), show that year
      if (fromYear === toYear) {
        return fromYear
      }
      // Otherwise show the most recent year in the range
      return toYear
    }
    // Default to current year for "All Time" or no filter
    return new Date().getFullYear().toString()
  }, [filters])

  // Filter dailyCounts to only show data for the selected year
  const data = useMemo(() => {
    return dailyCounts.filter(d => d.date.startsWith(year))
  }, [dailyCounts, year])
  
  // Get previous year's data for background comparison
  const prevYear = (parseInt(year) - 1).toString()
  const prevYearData = useMemo(() => {
    // Map previous year data to same day of week in current year
    return dailyCounts
      .filter(d => d.date.startsWith(prevYear))
      .map(d => {
        // Parse the date components
        const [prevYearStr, month, day] = d.date.split('-')
        
        // Get day of week for this date in previous year
        const prevDate = new Date(d.date)
        const prevDayOfWeek = prevDate.getDay()
        
        // Get day of week for same date in current year
        const currentYearSameDate = new Date(`${year}-${month}-${day}`)
        const currentYearDayOfWeek = currentYearSameDate.getDay()
        
        // Calculate how many days to shift
        let dayShift = prevDayOfWeek - currentYearDayOfWeek
        if (dayShift > 3) dayShift -= 7
        if (dayShift < -3) dayShift += 7
        
        // Apply the shift
        const shiftedDate = new Date(currentYearSameDate)
        shiftedDate.setDate(shiftedDate.getDate() + dayShift)
        
        // Format back to YYYY-MM-DD
        const shiftedDateStr = shiftedDate.toISOString().slice(0, 10)
        
        return {
          ...d,
          date: shiftedDateStr
        }
      })
      // Keep all dates that fall within the current year
      .filter(d => d.date.startsWith(year))
  }, [dailyCounts, prevYear, year])
  
  // Get max values for better color scaling
  const maxCount = Math.max(1, Math.max(...data.map(d => d.count)))
  const maxAmount = Math.max(1, Math.max(...data.map(d => d.total)))
  
  const opt: EChartsOption = useMemo(() => ({
    tooltip: { 
      position: 'top',
      formatter: (params: any) => {
        const dateStr = Array.isArray(params.data) ? params.data[0] : params.value[0]
        const value = Array.isArray(params.data) ? params.data[1] : params.value[1]
        
        // Check if this is from scatter series (previous year) or heatmap (current year)
        const isPrevYear = params.seriesIndex === 0
        
        // Find the actual data
        const item = isPrevYear 
          ? prevYearData.find(d => d.date === dateStr)
          : data.find(d => d.date === dateStr)
        
        if (!item) return ''
        
        const date = new Date(dateStr).toLocaleDateString('en-US', { 
          weekday: 'short', 
          month: 'short', 
          day: 'numeric'
        })
        
        let content = `<div style="padding: 8px">`
        
        if (isPrevYear) {
          content += `
            <div style="font-weight: bold; color: #fb923c">${date} (${prevYear} data)</div>
            <div>${item.count.toLocaleString()} Purchase Orders</div>
            <div style="color: #fb923c">${formatUSDCompact(item.total)}</div>
          `
        } else {
          content += `
            <div style="font-weight: bold">${date}, ${year}</div>
            <div>${item.count.toLocaleString()} Purchase Orders</div>
            <div style="color: #10b981">${formatUSDCompact(item.total)}</div>
          `
        }
        
        content += '</div>'
        return content
      }
    },
    visualMap: [
      // Visual map for previous year data (orange) - bottom slider
      {
        min: 0,
        max: maxCount,
        calculable: true,
        orient: 'horizontal', 
        left: '10%',
        right: '55%',
        bottom: 0,
        inRange: {
          color: ['rgba(251, 146, 60, 0.1)', 'rgba(251, 146, 60, 0.8)']
        },
        text: [prevYear, ''],
        textStyle: {
          color: '#fb923c',
          fontSize: 10
        },
        seriesIndex: 0,
        dimension: 1,
        handleStyle: {
          color: '#fb923c'
        },
        outOfRange: {
          color: 'transparent'
        }
      },
      // Visual map for current year data (blue-purple) - bottom slider
      {
        min: 0, 
        max: maxCount,
        calculable: true, 
        orient: 'horizontal', 
        left: '55%',
        right: '10%',
        bottom: 0,
        inRange: {
          color: ['#1a1f2e', '#3b4252', '#6366f1', '#8b5cf6', '#d946ef']
        },
        text: [year, ''],
        textStyle: {
          color: '#6366f1',
          fontSize: 10
        },
        seriesIndex: 1,
        dimension: 1,
        handleStyle: {
          color: '#6366f1'
        },
        outOfRange: {
          color: 'transparent'
        }
      }
    ],
    calendar: { 
      range: year, 
      top: 30, 
      left: 40, 
      right: 40, 
      bottom: 50, 
      cellSize: ['auto', 18],
      splitLine: {
        show: true,
        lineStyle: {
          color: '#2e3447',
          width: 2
        }
      },
      itemStyle: {
        borderWidth: 2,
        borderColor: '#1a1f2e'
      },
      yearLabel: {
        show: true,
        margin: 40,
        fontSize: 14,
        color: '#94a3b8'
      },
      monthLabel: {
        show: true,
        fontSize: 11,
        color: '#94a3b8'
      },
      dayLabel: {
        show: true,
        fontSize: 10,
        color: '#64748b',
        firstDay: 0 // Start week on Sunday
      }
    },
    series: [
      // Previous year data (all dates)
      {
        type: 'heatmap',
        coordinateSystem: 'calendar',
        data: prevYearData.map(pd => [pd.date, pd.count]),
        itemStyle: {
          borderColor: '#2e3447',
          borderWidth: 1
        }
      },
      // Current year data
      {
        type: 'heatmap',
        coordinateSystem: 'calendar',
        data: data.map(d => [d.date, d.count])
      }
    ]
  }), [data, prevYearData, year, prevYear, maxCount])

  return <ReactECharts option={opt} style={{ width: '100%', height: 280 }} />
}