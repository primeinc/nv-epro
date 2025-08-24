import { ReactNode } from 'react'

export default function MetricCard(props: { title: string; value: string | number; subtitle?: string; icon?: ReactNode; }) {
  const { title, value, subtitle, icon } = props
  return (
    <div className="metric-card">
      <div className="title">{title}</div>
      <div className="value">{value}</div>
      {subtitle && <div className="subtitle">{subtitle}</div>}
      {icon && <div className="icon" aria-hidden>{icon}</div>}
    </div>
  )
}