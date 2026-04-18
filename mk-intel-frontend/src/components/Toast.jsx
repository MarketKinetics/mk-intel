export function ToastContainer({ toasts, onRemove }) {
  if (!toasts.length) return null
  return (
    <div style={{ position: 'fixed', bottom: '24px', right: '24px', zIndex: 1000, display: 'flex', flexDirection: 'column', gap: '8px' }}>
      {toasts.map(toast => (
        <div key={toast.id} className={`flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium shadow-lg
          ${toast.type === 'success' ? 'bg-navy-900 text-white' : ''}
          ${toast.type === 'error' ? 'bg-red-600 text-white' : ''}
          ${toast.type === 'info' ? 'bg-navy-900 text-white' : ''}
        `}>
          <div className={`w-2 h-2 rounded-full flex-shrink-0
            ${toast.type === 'success' ? 'bg-teal-accent' : ''}
            ${toast.type === 'error' ? 'bg-red-300' : ''}
            ${toast.type === 'info' ? 'bg-teal-accent' : ''}
          `} />
          <span>{toast.message}</span>
          <button onClick={() => onRemove(toast.id)} className="ml-2 opacity-60 hover:opacity-100 text-white">✕</button>
        </div>
      ))}
    </div>
  )
}
