import { Outlet, NavLink, useNavigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useStore } from './lib/state'
import { DuckDBDataSource } from './lib/duckdbDataSource'
import TopNav from './components/Layout/TopNav'

export default function App() {
  const setPOs = useStore(s => s.setPOs)
  const navigate = useNavigate()

  useEffect(() => {
    const ds = new DuckDBDataSource()
    ds.load().then(setPOs).catch(err => {
      console.error(err)
      // Fallback: navigate to 404 to signal load issue
      navigate('/__error__')
    })
  }, [setPOs, navigate])

  return (
    <div className="stack">
      <TopNav />
      <div className="container grid-main">
        <aside className="sidenav">
          <nav className="stack">
            <NavLink to="/" end className={({isActive}) => isActive ? 'active' : ''}>Dashboard</NavLink>
            <NavLink to="/vendors" className={({isActive}) => isActive ? 'active' : ''}>Vendors</NavLink>
            <NavLink to="/purchase-orders" className={({isActive}) => isActive ? 'active' : ''}>Purchase Orders</NavLink>
          </nav>
        </aside>
        <main className="stack">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
