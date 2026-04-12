import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { sessions as sessionsApi } from '../api/client'

function s(val) {
  if (val === null || val === undefined) return ''
  if (typeof val === 'string') return val
  if (typeof val === 'number' || typeof val === 'boolean') return String(val)
  if (Array.isArray(val)) return val.map(v => s(v)).filter(Boolean).join(', ')
  if (typeof val === 'object') {
    return val.statement || val.description || val.premise || val.consequence ||
           val.assessment || val.reason || val.text || val.content || val.value ||
           val.action || val.name || ''
  }
  return ''
}

const CONFIDENCE_LABELS = {
  A: 'Full census alignment — high confidence',
  B1: 'Income divergence — income descriptors adjusted',
  B2: 'Race divergence — cultural layer adjusted',
  C: 'Full conflict — custom archetype, confidence penalty applied',
}

const DIM_KEYS = ['effectiveness', 'susceptibility', 'vulnerability', 'accessibility']
const DIM_SHORT = ['Eff.', 'Susc.', 'Vuln.', 'Acc.']
const DIM_FULL = ['Effectiveness', 'Susceptibility', 'Vulnerability', 'Accessibility']

const SECTIONS = [
  { id: 'scoring', label: 'Scoring' },
  { id: 'narrative', label: 'Messaging' },
  { id: 'actions', label: 'Actions' },
  { id: 'assessment', label: 'Assessment' },
]

function RadarChart({ scores, rank }) {
  const dims = DIM_KEYS.map((k, i) => ({ label: DIM_SHORT[i], val: scores[k] ?? 0 }))
  const cx = 80, cy = 80, r = 60
  const n = dims.length
  const points = dims.map((d, i) => {
    const angle = (i / n) * 2 * Math.PI - Math.PI / 2
    return {
      x: cx + r * d.val * Math.cos(angle),
      y: cy + r * d.val * Math.sin(angle),
      lx: cx + (r + 18) * Math.cos(angle),
      ly: cy + (r + 18) * Math.sin(angle),
    }
  })
  const gridLevels = [0.25, 0.5, 0.75, 1.0]
  const color = rank === 1 ? '#0D7377' : '#9CA3AF'
  const fillColor = rank === 1 ? 'rgba(13,115,119,0.15)' : 'rgba(156,163,175,0.15)'
  return (
    <div className="flex items-center gap-6">
      <svg width="160" height="160" viewBox="0 0 160 160">
        {gridLevels.map(level => {
          const gpts = dims.map((_, i) => {
            const angle = (i / n) * 2 * Math.PI - Math.PI / 2
            return `${cx + r * level * Math.cos(angle)},${cy + r * level * Math.sin(angle)}`
          }).join(' ')
          return <polygon key={level} points={gpts} fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
        })}
        {dims.map((_, i) => {
          const angle = (i / n) * 2 * Math.PI - Math.PI / 2
          return <line key={i} x1={cx} y1={cy} x2={cx + r * Math.cos(angle)} y2={cy + r * Math.sin(angle)} stroke="#E5E7EB" strokeWidth="0.5" />
        })}
        <polygon points={points.map(p => `${p.x},${p.y}`).join(' ')} fill={fillColor} stroke={color} strokeWidth="1.5" />
        {points.map((p, i) => <circle key={i} cx={p.x} cy={p.y} r="3" fill={color} />)}
        {points.map((p, i) => (
          <text key={i} x={p.lx} y={p.ly} textAnchor="middle" dominantBaseline="middle" fontSize="9" fill="#5C6B7A" fontFamily="Inter, system-ui">{dims[i].label}</text>
        ))}
      </svg>
      <div className="flex flex-col gap-2">
        {dims.map((d, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="text-xs text-slate w-24">{DIM_FULL[i]}</span>
            <span className="text-xs font-medium text-ink">{d.val.toFixed(2)}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function TARCard({ tar, ranking, sessionId, sobjStatement }) {
  const [expanded, setExpanded] = useState(false)
  const [tarData, setTarData] = useState(null)
  const [loadingTar, setLoadingTar] = useState(false)
  const [activeSection, setActiveSection] = useState('scoring')

  const rank = ranking?.rank ?? null
  const score = ranking?.final_score ?? null
  const dims = ranking?.dimension_scores ?? {}

  async function handleClick() {
    if (expanded) { setExpanded(false); return }
    if (tarData) { setExpanded(true); return }
    setLoadingTar(true)
    try {
      const r = await sessionsApi.getTar(sessionId, tar.tar_id)
      setTarData(r.data)
      setExpanded(true)
    } catch (e) {
      console.error(e)
    } finally {
      setLoadingTar(false)
    }
  }

  const audienceName = tar.audience_name || tarData?.header?.target_audience?.definition || tar.ta_id

  return (
    <div className={`bg-white border rounded-xl overflow-hidden transition-all ${rank === 1 ? 'border-teal-accent/40' : 'border-gray-200/80'}`}>
      <div className="p-5 cursor-pointer" onClick={handleClick}>
        <div className="flex items-start justify-between gap-3">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1.5 flex-wrap">
              {rank && score && (
                <span className={`text-xs px-2.5 py-1 rounded-full font-medium border ${
                  rank === 1 ? 'bg-teal-accent/15 text-teal-dark border-teal-accent/30' :
                  rank === 2 ? 'bg-green-50 text-green-700 border-green-200' :
                  rank === 3 ? 'bg-amber-50 text-amber-700 border-amber-200' :
                  'bg-gray-100 text-slate border-gray-200'}`}>
                  #{rank} · {score.toFixed(3)}
                </span>
              )}
              {rank === 1 && <span className="text-xs bg-teal-dark text-white px-2 py-0.5 rounded-full font-medium">First priority</span>}
            </div>
            <div className="text-base font-medium text-ink mb-1">{audienceName}</div>
            <div className="text-xs text-slate">{sobjStatement || tar.sobj_id}</div>
          </div>
          <div className="flex items-center gap-2">
            {loadingTar && <div className="w-4 h-4 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin" />}
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className={`transition-transform duration-200 flex-shrink-0 ${expanded ? 'rotate-180' : ''}`}>
              <path d="M4 6l4 4 4-4" stroke="#5C6B7A" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
        </div>
        {score && !expanded && rank && (
          <div className="mt-3 pt-3 border-t border-gray-100">
            <RadarChart scores={dims} rank={rank} />
          </div>
        )}
      </div>

      {expanded && tarData && (
        <div className="border-t border-gray-100">
          <div className="flex items-center justify-between px-5 pt-3 pb-0 border-b border-gray-100">
            <div className="flex gap-1 overflow-x-auto">
              {SECTIONS.map(sec => (
                <button key={sec.id} onClick={e => { e.stopPropagation(); setActiveSection(sec.id) }}
                  className={`px-3 py-2 text-xs font-medium rounded-t-lg whitespace-nowrap transition-all border-b-2 -mb-px
                    ${activeSection === sec.id ? 'text-teal-dark border-teal-dark bg-teal-accent/5' : 'text-slate border-transparent hover:text-ink'}`}>
                  {sec.label}
                </button>
              ))}
            </div>
            <Link
              to={`/session/${sessionId}/tars/${tar.tar_id}`}
              onClick={e => e.stopPropagation()}
              className="flex items-center gap-1 text-xs text-slate hover:text-teal-dark transition-colors whitespace-nowrap pb-2 ml-4 flex-shrink-0">
              Full report
              <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                <path d="M2 5h6M5 2l3 3-3 3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
            </Link>
          </div>
          <div className="p-5">
            {activeSection === 'scoring' && (
              <div className="space-y-5">
                <div>
                  <div className="text-xs font-medium text-slate uppercase tracking-widest mb-4">Score breakdown</div>
                  <RadarChart scores={dims} rank={rank} />
                </div>
                {ranking?.recommendation && (
                  <div className={`p-3 rounded-lg border-l-4 text-sm font-medium leading-relaxed
                    ${rank === 1 ? 'bg-green-50 border-green-500 text-green-800' :
                      rank === 2 ? 'bg-blue-50 border-blue-500 text-blue-800' :
                      'bg-amber-50 border-amber-500 text-amber-800'}`}>
                    {s(ranking.recommendation)}
                  </div>
                )}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  {[
                    { label: 'Effectiveness rating', val: tarData.effectiveness?.rating ? `${tarData.effectiveness.rating}/5` : null },
                    { label: 'Desired behavior', val: s(tarData.effectiveness?.desired_behavior?.statement || tarData.effectiveness?.desired_behavior) },
                    { label: 'Confidence', val: CONFIDENCE_LABELS[tarData.confidence_case] || `Case ${tarData.confidence_case}` },
                  ].filter(item => item.val).map(item => (
                    <div key={item.label} className="bg-surface rounded-lg p-3">
                      <div className="text-xs text-slate mb-1">{item.label}</div>
                      <div className="text-xs font-medium text-ink leading-relaxed">{String(item.val)}</div>
                    </div>
                  ))}
                </div>
                {tarData.effectiveness?.rating_rationale && (
                  <div>
                    <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Effectiveness rationale</div>
                    <p className="text-xs text-slate leading-relaxed">{s(tarData.effectiveness.rating_rationale)}</p>
                  </div>
                )}
              </div>
            )}
            {activeSection === 'narrative' && tarData.narrative_and_actions && (
              <div className="space-y-4">
                {tarData.narrative_and_actions.main_argument && (
                  <div>
                    <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Main argument</div>
                    <p className="text-sm font-medium text-ink leading-relaxed">{s(tarData.narrative_and_actions.main_argument)}</p>
                  </div>
                )}
                {tarData.narrative_and_actions.supporting_arguments?.length > 0 && (
                  <div>
                    <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Supporting arguments</div>
                    <ul className="space-y-2">
                      {tarData.narrative_and_actions.supporting_arguments.map((a, i) => {
                        const text = s(a)
                        if (!text) return null
                        return (
                          <li key={i} className="flex items-start gap-2 text-xs text-slate leading-relaxed">
                            <span className="text-teal-dark font-medium flex-shrink-0 mt-0.5">→</span>
                            <span>{text}</span>
                          </li>
                        )
                      })}
                    </ul>
                  </div>
                )}
              </div>
            )}
            {activeSection === 'actions' && tarData.narrative_and_actions?.recommended_actions && (
              <div className="space-y-3">
                <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">Recommended actions</div>
                {tarData.narrative_and_actions.recommended_actions.map((a, i) => (
                  <div key={i} className="flex items-start gap-3 p-3 bg-surface rounded-lg">
                    <div className="w-6 h-6 rounded-full bg-teal-dark text-white flex items-center justify-center text-xs font-medium flex-shrink-0">{i + 1}</div>
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-medium text-ink mb-1">{s(typeof a === 'string' ? a : a.action || a)}</div>
                      {typeof a === 'object' && a.channel && (
                        <div className="flex gap-2 flex-wrap">
                          <span className="text-xs bg-teal-accent/10 text-teal-dark px-2 py-0.5 rounded">{s(a.channel)}</span>
                          {a.timing && <span className="text-xs bg-gray-100 text-slate px-2 py-0.5 rounded">{s(a.timing)}</span>}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
            {activeSection === 'assessment' && tarData.assessment && (
              <div className="space-y-4">
                {[
                  { label: 'Baseline behavior', val: tarData.assessment.baseline_behavior },
                  { label: 'Target behavior', val: tarData.assessment.target_behavior },
                  { label: 'Assessment question', val: tarData.assessment.refined_assessment_question || tarData.assessment.initial_assessment_question },
                ].filter(item => item.val).map(item => (
                  <div key={item.label}>
                    <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">{item.label}</div>
                    <p className="text-xs text-slate leading-relaxed">{s(item.val)}</p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export function SessionDetail() {
  const { sessionId } = useParams()
  const [data, setData] = useState(null)
  const [tars, setTars] = useState([])
  const [rankings, setRankings] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedSobj, setSelectedSobj] = useState(null)
  const [sobjStatements, setSobjStatements] = useState({})

  useEffect(() => {
    async function load() {
      try {
        const [sessionRes, tarsRes, rankingsRes] = await Promise.all([
          sessionsApi.get(sessionId),
          sessionsApi.getTars(sessionId),
          sessionsApi.getRankings(sessionId),
        ])
        setData(sessionRes.data)
        setTars(tarsRes.data)
        setRankings(rankingsRes.data)
        const sobjs = Object.keys(rankingsRes.data)
        if (sobjs.length) setSelectedSobj(sobjs[0])

        // Get sobj statements from first tar per sobj
        const statements = {}
        const seenSobjs = new Set()
        for (const tar of tarsRes.data) {
          if (!seenSobjs.has(tar.sobj_id)) {
            seenSobjs.add(tar.sobj_id)
            try {
              const tarRes = await sessionsApi.getTar(sessionId, tar.tar_id)
              statements[tar.sobj_id] = tarRes.data.sobj_statement || tar.sobj_id
            } catch { statements[tar.sobj_id] = tar.sobj_id }
          }
        }
        setSobjStatements(statements)
      } catch (e) {
        setError('Could not load this analysis.')
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [sessionId])

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: '#F8F7F4' }}>
      <div className="flex items-center gap-2 text-sm text-slate">
        <div className="w-4 h-4 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin" />
        Loading your analysis...
      </div>
    </div>
  )

  if (error) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: '#F8F7F4' }}>
      <div className="text-sm text-slate">{error}</div>
    </div>
  )

  const currentRankings = rankings[selectedSobj] || []

  const getRankingForTar = (tarId) => {
    const taId = tarId.replace(/TAR-SOBJ[-_]\d+-/, '').replace(/TAR-SOBJ[-_]\d+_/, '')
    return currentRankings.find(r => r.tar_id === taId) || null
  }

  const tarsForSobj = selectedSobj
    ? tars.filter(t => t.sobj_id === selectedSobj)
    : tars

  const sortedTars = [...tarsForSobj].sort((a, b) => {
    const ra = getRankingForTar(a.tar_id)?.rank ?? 99
    const rb = getRankingForTar(b.tar_id)?.rank ?? 99
    return ra - rb
  })

  const sobjs = Object.keys(rankings)

  return (
    <div className="min-h-screen" style={{ background: '#F8F7F4' }}>
      <div style={{ background: 'radial-gradient(ellipse at 50% -20%, #102847 0%, #0A1628 60%)' }} className="px-6 py-10">
        <div className="max-w-4xl mx-auto">
          <Link to="/setup" className="inline-flex items-center gap-1.5 text-xs text-white/40 hover:text-white/70 transition-colors mb-6">
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M8 2L4 6l4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
            New analysis
          </Link>
          <div className="flex items-start justify-between flex-wrap gap-4">
            <div>
              <div className="text-xs font-medium text-teal-accent/60 uppercase tracking-widest mb-2">{data?.obj_statement || 'Analysis'}</div>
              <h1 className="text-2xl sm:text-3xl font-medium text-white/95 tracking-tight mb-2">{data?.company_name || 'Your analysis'}</h1>
              <p className="text-sm text-white/45">Live analysis · {tars.length} reports generated</p>
            </div>
            <div className="flex items-center gap-4">
              <div className="flex gap-6">
                {[
                  { val: tars.length, label: 'reports' },
                  { val: sobjs.length, label: 'objectives' },
                ].map(m => (
                  <div key={m.label} className="text-center">
                    <div className="text-xl font-medium text-white/90">{m.val}</div>
                    <div className="text-xs text-white/40">{m.label}</div>
                  </div>
                ))}
              </div>
              <a
                href={`${import.meta.env.VITE_API_URL || 'https://web-production-7ec13.up.railway.app'}/sessions/${sessionId}/export`}
                download
                className="flex items-center gap-1.5 text-xs font-medium bg-teal-dark text-white px-3 py-1.5 rounded-lg hover:opacity-90 transition-opacity">
                <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
                  <path d="M6.5 1v7M3.5 5l3 3 3-3M1 10h11" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
                Download data
              </a>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-4xl mx-auto px-6 py-8">
        {sobjs.length > 1 && (
          <div className="flex gap-2 mb-6 flex-wrap">
            {sobjs.map(sobjId => {
              const label = sobjStatements[sobjId] || sobjId
              const shortLabel = label.length > 60 ? label.substring(0, 60) + '…' : label
              return (
                <button key={sobjId} onClick={() => setSelectedSobj(sobjId)}
                  className={`px-4 py-2 rounded-lg text-xs font-medium transition-all border
                    ${selectedSobj === sobjId ? 'bg-teal-dark text-white border-teal-dark' : 'bg-white text-slate border-gray-200 hover:border-gray-300'}`}>
                  {shortLabel}
                </button>
              )
            })}
          </div>
        )}

        {currentRankings.length > 0 && (
          <div className="bg-white border border-gray-200/60 rounded-xl p-5 mb-6">
            <div className="text-xs font-medium text-slate uppercase tracking-widest mb-4">
              Rankings — {sobjStatements[selectedSobj] || selectedSobj}
            </div>
            <div className="space-y-3">
              {currentRankings.map(r => {
                const tar = tars.find(t => {
                  const taId = t.tar_id.replace(/TAR-SOBJ[-_]\d+-/, '').replace(/TAR-SOBJ[-_]\d+_/, '')
                  return taId === r.tar_id
                })
                return (
                  <div key={r.tar_id} className={`flex items-center gap-3 p-3 rounded-lg ${r.rank === 1 ? 'bg-teal-accent/5 border border-teal-accent/20' : 'bg-surface'}`}>
                    <span className={`text-sm font-medium w-6 flex-shrink-0 ${r.rank === 1 ? 'text-teal-dark' : 'text-slate'}`}>#{r.rank}</span>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-ink mb-2 leading-tight">{tar?.audience_name || r.tar_id}</div>
                      <div className="grid grid-cols-4 gap-2">
                        {DIM_KEYS.map((k, i) => (
                          <div key={k}>
                            <div className="text-xs text-slate mb-0.5">{DIM_SHORT[i]}</div>
                            <div className="w-full bg-gray-200 rounded-full h-1">
                              <div className={`h-1 rounded-full ${r.rank === 1 ? 'bg-teal-dark' : 'bg-gray-400'}`}
                                style={{ width: `${(r.dimension_scores?.[k] ?? 0) * 100}%` }} />
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                    <div className="text-right flex-shrink-0">
                      <div className="text-sm font-medium text-ink">{r.final_score?.toFixed(3)}</div>
                      <div className="text-xs text-slate">score</div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">Target audience reports</div>
        <div className="space-y-3">
          {sortedTars.map(tar => (
            <TARCard key={tar.tar_id} tar={tar} ranking={getRankingForTar(tar.tar_id)} sessionId={sessionId} sobjStatement={sobjStatements[tar.sobj_id]} />
          ))}
        </div>
      </div>
    </div>
  )
}
