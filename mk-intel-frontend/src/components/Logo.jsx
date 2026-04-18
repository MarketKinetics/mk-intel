export function Logo({ dark = false, size = 20 }) {
  const accent = '#14C9B8'
  const word = dark ? '#F8F7F4' : '#0F1923'

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
      <svg width={size} height={size} viewBox="0 0 20 20" fill="none">
        <path d="M10 2 A8 8 0 0 1 18 10" stroke={accent} strokeWidth="2.5" strokeLinecap="round"/>
        <path d="M18 10 A8 8 0 0 1 10 18" stroke={accent} strokeWidth="2.5" strokeLinecap="round" opacity=".6"/>
        <path d="M10 18 A8 8 0 0 1 2 10" stroke={accent} strokeWidth="2.5" strokeLinecap="round" opacity=".3"/>
        <path d="M2 10 A8 8 0 0 1 10 2" stroke={accent} strokeWidth="2.5" strokeLinecap="round" opacity=".15"/>
        <circle cx="10" cy="10" r="2.5" fill={accent}/>
      </svg>
      <span style={{
        fontSize: '15px',
        fontWeight: 500,
        letterSpacing: '-.01em',
        color: word,
        fontFamily: 'Inter, system-ui, sans-serif',
      }}>
        MK <span style={{ color: accent }}>Intel</span>
      </span>
    </div>
  )
}
