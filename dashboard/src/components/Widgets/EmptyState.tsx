export default function EmptyState({ text }: { text: string }) {
  return <div className="surface" style={{padding:24, textAlign:'center', color:'var(--muted)'}}>{text}</div>
}