import { Link, useLocation } from 'react-router-dom'
import { Logo } from './Logo'

export function Navbar({ session }) {
  const location = useLocation()
  const isApp = location.pathname !== '/'

  return (
    <nav style={{ background: isApp ? 'white' : '#0A1628' }}
      className={`px-8 h-14 flex items-center justify-between flex-shrink-0 ${isApp ? 'border-b border-gray-200' : ''}`}>
      <Link to="/"><Logo dark={!isApp} /></Link>
      <div className="flex items-center gap-6">
        {!isApp && (
          <>
            <Link to="/examples" className="text-sm text-white/40 hover:text-white/70 transition-colors">Examples</Link>
            <Link to="/setup" className="bg-teal-accent text-navy-900 text-sm font-medium px-4 py-1.5 rounded-md hover:opacity-90 transition-opacity">
              Try free
            </Link>
          </>
        )}
        {isApp && session && (
          <span className="text-xs text-slate bg-surface px-3 py-1 rounded">
            {session.company_name || 'New analysis'}
          </span>
        )}
        {isApp && (
          <Link to="/" className="text-xs text-slate hover:text-ink transition-colors">← Home</Link>
        )}
      </div>
    </nav>
  )
}
