import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import './styles/globals.css'
import App from './App'
import Dashboard from './routes/Dashboard'
import Vendors from './routes/Vendors'
import VendorDetail from './routes/VendorDetail'
import PurchaseOrders from './routes/PurchaseOrders'
import NotFound from './routes/NotFound'

const router = createBrowserRouter([
  { path: '/', element: <App />,
    children: [
      { index: true, element: <Dashboard /> },
      { path: 'vendors', element: <Vendors /> },
      { path: 'vendors/:name', element: <VendorDetail /> },
      { path: 'purchase-orders', element: <PurchaseOrders /> },
    ]
  },
  { path: '*', element: <NotFound /> }
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
)