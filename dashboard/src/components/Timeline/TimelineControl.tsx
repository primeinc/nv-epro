import { useStore } from '../../lib/state'
import dayjs from 'dayjs'

export default function TimelineControl() {
  const filters = useStore(s => s.filters)
  const setFilters = useStore(s => s.setFilters)
  const metrics = useStore(s => s.metrics)
  
  // Quick presets for common date ranges
  const presets = [
    { label: 'All Time', from: null, to: null },
    { label: 'Last 30 Days', from: dayjs().subtract(30, 'days').format('YYYY-MM-DD'), to: dayjs().format('YYYY-MM-DD') },
    { label: 'Last 90 Days', from: dayjs().subtract(90, 'days').format('YYYY-MM-DD'), to: dayjs().format('YYYY-MM-DD') },
    { label: 'Last 6 Months', from: dayjs().subtract(6, 'months').format('YYYY-MM-DD'), to: dayjs().format('YYYY-MM-DD') },
    { label: 'Last Year', from: dayjs().subtract(1, 'year').format('YYYY-MM-DD'), to: dayjs().format('YYYY-MM-DD') },
    { label: 'YTD', from: dayjs().startOf('year').format('YYYY-MM-DD'), to: dayjs().format('YYYY-MM-DD') },
    { label: '2024', from: '2024-01-01', to: '2024-12-31' },
    { label: '2023', from: '2023-01-01', to: '2023-12-31' },
    { label: '2022', from: '2022-01-01', to: '2022-12-31' },
    { label: '2021', from: '2021-01-01', to: '2021-12-31' },
    { label: '2020', from: '2020-01-01', to: '2020-12-31' },
    { label: '2019', from: '2019-01-01', to: '2019-12-31' },
    { label: '2018', from: '2018-01-31', to: '2018-12-31' }, // Data starts Jan 31, 2018
  ]
  
  const handlePresetClick = (from: string | null, to: string | null) => {
    setFilters({ date: { from, to } })
  }
  
  const handleFromChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFilters({ date: { ...filters.date, from: e.target.value || null } })
  }
  
  const handleToChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFilters({ date: { ...filters.date, to: e.target.value || null } })
  }
  
  const isActive = (from: string | null, to: string | null) => {
    return filters.date.from === from && filters.date.to === to
  }
  
  const handleOutlierToggle = () => {
    setFilters({ excludeOutliers: !filters.excludeOutliers })
  }
  
  return (
    <div className="timeline-control">
      <div className="timeline-header">
        <h3>Timeline</h3>
        {(filters.date.from || filters.date.to) && (
          <button 
            className="timeline-clear"
            onClick={() => handlePresetClick(null, null)}
            title="Clear date filter"
          >
            ✕
          </button>
        )}
      </div>
      
      <div className="timeline-presets">
        {presets.map(p => (
          <button
            key={p.label}
            className={`timeline-preset ${isActive(p.from, p.to) ? 'active' : ''}`}
            onClick={() => handlePresetClick(p.from, p.to)}
          >
            {p.label}
          </button>
        ))}
      </div>
      
      <div className="timeline-custom">
        <label>
          <span>From:</span>
          <input
            type="date"
            value={filters.date.from || ''}
            onChange={handleFromChange}
            min="2018-01-31"
          />
        </label>
        <label>
          <span>To:</span>
          <input
            type="date"
            value={filters.date.to || ''}
            onChange={handleToChange}
            min="2018-01-31"
          />
        </label>
      </div>
      
      <div className="timeline-outliers">
        <label className="timeline-checkbox">
          <input
            type="checkbox"
            checked={filters.excludeOutliers}
            onChange={handleOutlierToggle}
          />
          <span>Exclude outliers</span>
        </label>
        {filters.excludeOutliers && (
          <small className="muted">Using IQR method (Q1-1.5*IQR to Q3+1.5*IQR)</small>
        )}
      </div>
      
      {(filters.date.from || filters.date.to) && (
        <div className="timeline-info">
          <small className="muted">
            Showing {metrics.poCount.toLocaleString()} POs
            {filters.date.from && filters.date.to && 
              ` from ${dayjs(filters.date.from).format('MMM D, YYYY')} to ${dayjs(filters.date.to).format('MMM D, YYYY')}`
            }
          </small>
        </div>
      )}
    </div>
  )
}