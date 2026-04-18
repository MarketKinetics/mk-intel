import { Link } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { examples } from '../api/client'

const USE_CASES = ['Campaign targeting', 'Customer segmentation', 'Message testing', 'Market entry', 'Product positioning']

const STATS = [
  { val: '20M+', label: 'Population records', sub: 'Census & survey research' },
  { val: '7', label: 'Behavioral archetypes', sub: 'Clustering · profiling' },
  { val: '4-layer', label: 'Audience modeling', sub: 'Structural · Psych · Media · Behavioral' },
]

const FEATURES = [
  {
    title: 'Census-grounded archetypes',
    desc: 'Every segment is derived from population-scale data — not assumptions or demographic guesswork.',
    rotate: 0,
  },
  {
    title: 'Evidence-based scoring',
    desc: 'Four-dimensional algorithm scores each audience on effectiveness, susceptibility, vulnerability, and accessibility.',
    rotate: 90,
  },
  {
    title: 'Actionable reports',
    desc: 'Each report tells your team who the audience is, what motivates them, and exactly how to reach them.',
    rotate: 180,
  },
]

function ArcMark({ rotate = 0, size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none" style={{ transform: `rotate(${rotate}deg)` }}>
      <path d="M10 2 A8 8 0 0 1 18 10" stroke="#0D7377" strokeWidth="2.5" strokeLinecap="round"/>
      <path d="M18 10 A8 8 0 0 1 10 18" stroke="#0D7377" strokeWidth="2.5" strokeLinecap="round" opacity=".6"/>
      <path d="M10 18 A8 8 0 0 1 2 10" stroke="#0D7377" strokeWidth="2.5" strokeLinecap="round" opacity=".3"/>
      <path d="M2 10 A8 8 0 0 1 10 2" stroke="#0D7377" strokeWidth="2.5" strokeLinecap="round" opacity=".15"/>
      <circle cx="10" cy="10" r="2.5" fill="#0D7377"/>
    </svg>
  )
}

export function Landing() {
  const [exampleList, setExampleList] = useState([])

  useEffect(() => {
    examples.list().then(r => setExampleList(r.data)).catch(() => {})
  }, [])

  return (
    <div className="min-h-screen flex flex-col">

      <section
        className="flex flex-col items-center justify-center text-center px-6 py-24"
        style={{ background: 'radial-gradient(ellipse at 50% -10%, #102847 0%, #0A1628 65%)' }}
      >
        <div className="inline-flex items-center gap-2 border border-teal-accent/25 bg-teal-accent/10 text-teal-accent text-xs px-4 py-1.5 rounded-full mb-8">
          <span className="w-1.5 h-1.5 rounded-full bg-teal-accent inline-block" />
          AI-first audience intelligence
        </div>

        <h1 className="text-5xl font-medium text-white/95 leading-tight tracking-tight mb-6 max-w-2xl">
          Identify who matters.<br />
          <span className="text-teal-accent">Know how to move them.</span>
        </h1>

        <p className="text-white/45 text-base leading-relaxed max-w-xl mb-10">
          Turn your customer data into population-based audience archetypes — ranked, scored, and ready to act on.
        </p>

        <div className="flex gap-3 mb-16">
          <Link to="/setup"
            className="flex items-center gap-2 bg-teal-accent text-navy-900 text-sm font-medium px-6 py-3 rounded-lg hover:opacity-90 transition-opacity">
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path d="M7 1v8M4 6l3 3 3-3M2 11h10" stroke="#0A1628" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
            Analyze your data
          </Link>
          <Link to="/examples"
            className="border border-white/20 text-white/60 text-sm px-6 py-3 rounded-lg hover:border-white/40 hover:text-white/80 transition-all">
            Browse examples
          </Link>
        </div>

        <div className="flex gap-16 border-t border-white/8 pt-10 w-full max-w-2xl justify-center mb-8">
          {STATS.map(s => (
            <div key={s.val} className="text-center">
              <div className="text-2xl font-medium text-white/90 mb-1">{s.val}</div>
              <div className="text-xs text-white/55 mb-0.5">{s.label}</div>
              <div className="text-xs text-white/25">{s.sub}</div>
            </div>
          ))}
        </div>

        <div className="flex flex-col items-center gap-1">
          
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
            <path d="M4 6l4 4 4-4" stroke="#14C9B8" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
      </section>

      <section className="bg-white px-6 py-16">
        <div className="max-w-3xl mx-auto text-center">
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-5">
            A different kind of intelligence
          </div>
          <p className="text-2xl font-medium text-ink leading-snug tracking-tight max-w-2xl mx-auto mb-8">
            Unlike traditional analytics tools, MK Intel maps your customers to real-world population archetypes — so you understand not just what they do, but{' '}
            <span className="text-teal-dark">who they are and what moves them.</span>
          </p>
          <div className="flex flex-wrap gap-2 justify-center">
            {USE_CASES.map(u => (
              <span key={u} className="text-xs text-teal-dark bg-teal-accent/10 border border-teal-accent/20 px-4 py-1.5 rounded-full font-medium">
                {u}
              </span>
            ))}
          </div>
        </div>
      </section>

      <section className="px-6 py-14" style={{ background: '#F8F7F4' }}>
        <div className="max-w-3xl mx-auto">
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-6">
            Live examples
          </div>
          {exampleList.length === 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {['GlobalCart', 'CloudSync'].map(name => (
                <div key={name} className="bg-white border border-gray-200/80 rounded-xl p-5 animate-pulse">
                  <div className="h-4 bg-gray-100 rounded w-24 mb-3" />
                  <div className="h-3 bg-gray-100 rounded w-full mb-2" />
                  <div className="h-3 bg-gray-100 rounded w-3/4" />
                </div>
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {exampleList.map(ex => (
                <Link key={ex.slug} to={`/examples/${ex.slug}`}
                  className="bg-white border border-gray-200/80 rounded-xl p-5 hover:border-teal-dark/30 hover:shadow-sm transition-all group block">
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-sm font-medium text-ink">{ex.name}</span>
                    <span className="text-xs text-teal-dark bg-teal-accent/10 px-2 py-0.5 rounded font-medium">{ex.sector}</span>
                  </div>
                  <p className="text-xs text-slate leading-relaxed mb-4">{ex.description}</p>
                  <div className="flex gap-4 mb-3">
                    {[
                      { val: ex.ta_count, label: 'audiences' },
                      { val: ex.tar_count, label: 'reports' },
                      { val: ex.sobj_count, label: 'objectives' },
                    ].map(m => (
                      <div key={m.label} className="text-xs text-slate">
                        <span className="font-medium text-ink">{m.val}</span> {m.label}
                      </div>
                    ))}
                  </div>
                  <div className="text-xs text-teal-dark font-medium opacity-0 group-hover:opacity-100 transition-opacity">
                    View analysis →
                  </div>
                </Link>
              ))}
            </div>
          )}
        </div>
      </section>

      <section className="bg-white px-6 py-14">
        <div className="max-w-3xl mx-auto">
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-8">
            How it works
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
            {FEATURES.map((f) => (
              <div key={f.title} className="p-5 rounded-xl border border-gray-200/60">
                <div className="w-9 h-9 rounded-lg bg-teal-accent/10 flex items-center justify-center mb-4">
                  <ArcMark rotate={f.rotate} size={18} />
                </div>
                <div className="text-sm font-medium text-ink mb-2">{f.title}</div>
                <div className="text-xs text-slate leading-relaxed">{f.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <footer className="border-t border-gray-200/60 px-6 py-8" style={{ background: '#F8F7F4' }}>
        <div className="max-w-3xl mx-auto">
          <span className="text-xs text-slate">MK Intel · Market Kinetics platform</span>
        </div>
      </footer>

    </div>
  )
}
