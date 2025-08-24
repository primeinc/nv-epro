import { useStore } from '../../lib/state'
import { useEffect, useState } from 'react'

export default function TopNav() {
  const filters = useStore(s => s.filters)
  const setFilters = useStore(s => s.setFilters)
  const resetFilters = useStore(s => s.resetFilters)
  const [theme, setTheme] = useState<string>(() => document.documentElement.getAttribute('data-theme') ?? 'dark')

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  return (
    <header className="topnav">
      <div className="container topnav-inner">
        <div className="cluster">
          <strong>Nevada Procurement</strong>
          <span className="muted">Dashboard</span>
        </div>

        <div className="searchbar" style={{maxWidth: 860, flex: 1}}>
          <input
            placeholder="Search PO, vendor, dept, buyer, desc"
            value={filters.query}
            onChange={e => setFilters({ query: e.target.value })}
          />
          <select value={filters.status} onChange={e => setFilters({ status: e.target.value as any })}>
            <option value="All">All status</option>
            <option>Sent</option>
            <option>Complete</option>
            <option>Closed</option>
            <option>Partial</option>
          </select>
          <select value={filters.department} onChange={e => setFilters({ department: e.target.value as any })}>
            <option value="All">All departments</option>
            {useStore.getState().departments.map(d => <option key={d} value={d}>{d}</option>)}
          </select>
          <button className="btn btn-secondary" onClick={() => resetFilters()}>Reset</button>
        </div>

        <div className="cluster">
          <button className="btn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? 'Light' : 'Dark'} theme
          </button>
        </div>
      </div>
    </header>
  )
}