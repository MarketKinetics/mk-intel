import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { examples as examplesApi, sessions as sessionsApi } from '../api/client'

function safeStr(val) {
  if (val === null || val === undefined) return ''
  if (typeof val === 'string') return val
  if (typeof val === 'number') return String(val)
  if (typeof val === 'boolean') return String(val)
  if (typeof val === 'object') {
    const s = val.statement || val.description || val.assessment || val.reason ||
              val.premise || val.consequence || val.text || val.content || val.value || ''
    if (s) return s
    // If object has premise AND consequence, combine them
    if (val.premise && val.consequence) return `${val.premise} → ${val.consequence}`
    return ''
  }
  return ''
}

function safeRender(val) {
  const s = safeStr(val)
  return s || null
}

function Section({ number, title, children }) {
  return (
    <div className="bg-white border border-gray-200/60 rounded-xl overflow-hidden mb-4">
      <div className="px-6 py-4 border-b border-gray-100 bg-surface flex items-center gap-3">
        <div className="w-6 h-6 rounded-full bg-teal-dark text-white flex items-center justify-center text-xs font-medium flex-shrink-0">{number}</div>
        <div className="text-sm font-medium text-ink uppercase tracking-widest">{title}</div>
      </div>
      <div className="px-6 py-5 space-y-5">{children}</div>
    </div>
  )
}

function Field({ label, value }) {
  if (!value) return null
  const text = safeStr(value)
  if (!text) return null
  return (
    <div>
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">{label}</div>
      <p className="text-sm text-ink leading-relaxed">{text}</p>
    </div>
  )
}

function RatingBar({ label, value, max = 5 }) {
  if (!value) return null
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-slate w-36 flex-shrink-0">{label}</span>
      <div className="flex gap-1">
        {Array.from({ length: max }).map((_, i) => (
          <div key={i} className={`w-7 h-2 rounded-sm ${i < value ? 'bg-teal-dark' : 'bg-gray-200'}`} />
        ))}
      </div>
      <span className="text-xs font-medium text-ink">{value}/{max}</span>
    </div>
  )
}

function TagList({ label, items }) {
  if (!items?.length) return null
  const tags = items.map(item => safeStr(item)).filter(Boolean)
    .filter(Boolean)
  if (!tags.length) return null
  return (
    <div>
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">{label}</div>
      <div className="flex flex-wrap gap-2">
        {tags.map((t, i) => (
          <span key={i} className="text-xs bg-teal-accent/10 text-teal-dark px-2.5 py-1 rounded-full">{t}</span>
        ))}
      </div>
    </div>
  )
}



function ItemList({ label, items, color = 'default' }) {
  if (!items?.length) return null
  if (!Array.isArray(items)) return null
  const bg = color === 'green' ? 'bg-green-50' : color === 'red' ? 'bg-red-50' : 'bg-surface'
  const textColor = color === 'green' ? 'text-green-800' : color === 'red' ? 'text-red-800' : 'text-ink'
  return (
    <div>
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">{label}</div>
      <div className="space-y-2">
        {items.map((item, i) => {
          const text = safeStr(item)
          const sub = typeof item === 'object' ? safeStr(item.salience_to_ta || item.severity_to_ta || item.behavioral_link || '') : ''
          const badge = typeof item === 'object' ? safeStr(item.priority || item.category || '') : ''
          if (!text) return null
          return (
            <div key={i} className={`${bg} rounded-lg p-3`}>
              <div className="flex items-start gap-2">
                {badge && <span className={`text-xs px-2 py-0.5 rounded-full font-medium flex-shrink-0 mt-0.5 ${badge === 'critical' ? 'bg-teal-accent/15 text-teal-dark' : 'bg-gray-100 text-slate'}`}>{badge}</span>}
                <p className={`text-xs ${textColor} leading-relaxed`}>{text}</p>
              </div>
              {sub && typeof sub === 'string' && sub.length > 0 && (
                <p className="text-xs text-slate leading-relaxed mt-1.5">{sub}</p>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function EffectivenessSection({ data }) {
  if (!data) return null
  const desiredBehavior = data.desired_behavior?.statement || data.desired_behavior
  const sobjDesc = data.sobj_impact?.description
  const sobjContrib = data.sobj_impact?.estimated_contribution
  return (
    <Section number="1" title="Effectiveness">
      <RatingBar label="Rating" value={data.rating} />
      {desiredBehavior && <Field label="Desired behavior" value={desiredBehavior} />}
      {sobjDesc && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">SOBJ impact</div>
          <p className="text-sm text-slate leading-relaxed">{sobjDesc}</p>
          {sobjContrib && <p className="text-sm text-slate leading-relaxed mt-2">{sobjContrib}</p>}
        </div>
      )}
      {data.rating_rationale && <Field label="Rationale" value={data.rating_rationale} />}
      {data.restrictions?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Restrictions</div>
          <div className="space-y-2">
            {data.restrictions.map((r, i) => (
              <div key={i} className={`p-3 rounded-lg text-xs leading-relaxed ${r.severity === 'high' ? 'bg-red-50 text-red-800' : 'bg-amber-50 text-amber-800'}`}>
                <span className="font-medium">{r.type || r.category}: </span>
                {r.description || r.constraint || String(r)}
              </div>
            ))}
          </div>
        </div>
      )}
    </Section>
  )
}

function ConditionsSection({ data }) {
  if (!data) return null
  const currentBehavior = data.current_behavior?.statement || data.current_behavior
  return (
    <Section number="2" title="Conditions">
      {currentBehavior && <Field label="Current behavior" value={currentBehavior} />}
      {data.external_conditions?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">External conditions</div>
          <div className="space-y-2">
            {data.external_conditions.map((c, i) => (
              <div key={i} className="bg-surface rounded-lg p-3">
                <div className="text-xs font-medium text-ink mb-1">{c.id}: {c.description || c.statement}</div>
                {c.relevance && <p className="text-xs text-slate leading-relaxed">{c.relevance}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
      {data.internal_conditions?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Internal conditions</div>
          <div className="space-y-2">
            {data.internal_conditions.map((c, i) => (
              <div key={i} className="bg-surface rounded-lg p-3">
                <div className="text-xs font-medium text-ink mb-1">{c.id}: {c.description || c.statement}</div>
                {c.relevance && <p className="text-xs text-slate leading-relaxed">{c.relevance}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
      <ItemList label="Positive consequences" items={data.positive_consequences} color="green" />
      <ItemList label="Negative consequences" items={data.negative_consequences} color="red" />
    </Section>
  )
}

function VulnerabilitiesSection({ data }) {
  if (!data) return null
  return (
    <Section number="3" title="Vulnerabilities">
      {data.motives?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Motives</div>
          <div className="space-y-2">
            {data.motives.map((m, i) => (
              <div key={i} className={`p-3 rounded-lg border ${m.priority === 'critical' ? 'border-teal-accent/30 bg-teal-accent/5' : 'border-gray-100 bg-surface'}`}>
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <span className="text-xs font-medium text-ink">{m.id}: {m.description}</span>
                  {m.priority && <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${m.priority === 'critical' ? 'bg-teal-accent/15 text-teal-dark' : 'bg-gray-100 text-slate'}`}>{m.priority}</span>}
                </div>
                {m.behavioral_link && <p className="text-xs text-slate leading-relaxed">{m.behavioral_link}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
      {data.psychographics?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Psychographics</div>
          <div className="space-y-2">
            {data.psychographics.map((p, i) => (
              <div key={i} className="bg-surface rounded-lg p-3">
                <div className="text-xs font-medium text-ink mb-1">{p.id}: {p.description}</div>
                {p.behavioral_link && <p className="text-xs text-slate leading-relaxed">{p.behavioral_link}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
      {data.symbols_and_cues?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Symbols & cues</div>
          <div className="space-y-2">
            {data.symbols_and_cues.map((s, i) => (
              <div key={i} className="bg-surface rounded-lg p-3">
                <p className="text-xs text-ink leading-relaxed">{s.description || String(s)}</p>
                {s.behavioral_link && <p className="text-xs text-slate leading-relaxed mt-1">{s.behavioral_link}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
    </Section>
  )
}

function SusceptibilitySection({ data }) {
  if (!data) return null
  return (
    <Section number="4" title="Susceptibility">
      <RatingBar label="Rating" value={data.rating} />
      {data.rating_rationale && <Field label="Rationale" value={data.rating_rationale} />}
      {data.recommended_approach?.primary_approach && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Recommended approach</div>
          <div className="flex gap-2 flex-wrap mb-2">
            <span className="text-xs bg-teal-dark text-white px-3 py-1.5 rounded-lg font-medium">{data.recommended_approach.primary_approach}</span>
            {data.recommended_approach.secondary_approach && (
              <span className="text-xs bg-teal-accent/10 text-teal-dark px-3 py-1.5 rounded-lg font-medium">{data.recommended_approach.secondary_approach}</span>
            )}
          </div>
          {data.recommended_approach.sequencing_note && (
            <p className="text-xs text-slate leading-relaxed">{data.recommended_approach.sequencing_note}</p>
          )}
        </div>
      )}
      <ItemList label="Perceived rewards" items={Array.isArray(data.perceived_rewards) ? data.perceived_rewards : []} color="green" />
      <ItemList label="Perceived risks" items={Array.isArray(data.perceived_risks) ? data.perceived_risks : []} color="red" />
      {data.value_belief_alignment?.assessment && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Value & belief alignment</div>
          <p className="text-sm text-slate leading-relaxed">{data.value_belief_alignment.assessment}</p>
        </div>
      )}
      {data.audience_priority_recommendation?.reason && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Priority recommendation</div>
          <p className="text-sm text-slate leading-relaxed">{data.audience_priority_recommendation.reason}</p>
        </div>
      )}
    </Section>
  )
}

function AccessibilitySection({ data }) {
  if (!data?.length) return null
  return (
    <Section number="5" title="Accessibility">
      <div className="space-y-4">
        {data.map((channel, i) => (
          <div key={i} className="border border-gray-100 rounded-lg overflow-hidden">
            <div className="flex items-center justify-between px-4 py-3 bg-surface">
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium text-ink">{channel.channel_name}</span>
                <span className="text-xs text-slate bg-gray-100 px-2 py-0.5 rounded">{channel.channel_type}</span>
              </div>
              <div className="flex gap-1">
                {Array.from({ length: 5 }).map((_, j) => (
                  <div key={j} className={`w-4 h-1.5 rounded-sm ${j < (channel.reach_quality || 0) ? 'bg-teal-dark' : 'bg-gray-200'}`} />
                ))}
              </div>
            </div>
            <div className="px-4 py-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
              {channel.advantages?.length > 0 && (
                <div>
                  <div className="text-xs text-slate mb-1.5 font-medium">Advantages</div>
                  <ul className="space-y-1">
                    {channel.advantages.map((a, j) => (
                      <li key={j} className="text-xs text-slate flex items-start gap-1.5 leading-relaxed">
                        <span className="text-green-600 flex-shrink-0 font-bold">+</span>{a}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {channel.disadvantages?.length > 0 && (
                <div>
                  <div className="text-xs text-slate mb-1.5 font-medium">Disadvantages</div>
                  <ul className="space-y-1">
                    {channel.disadvantages.map((d, j) => (
                      <li key={j} className="text-xs text-slate flex items-start gap-1.5 leading-relaxed">
                        <span className="text-red-500 flex-shrink-0 font-bold">−</span>{d}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
            {channel.constraints && (
              <div className="px-4 pb-3">
                <div className="text-xs text-amber-700 bg-amber-50 rounded p-2 leading-relaxed">
                  <span className="font-medium">Constraints: </span>{channel.constraints}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </Section>
  )
}

function NarrativeSection({ data }) {
  if (!data) return null
  const mainArg = safeStr(data.main_argument)
  return (
    <Section number="6" title="Narrative & Actions">
      {mainArg && (
        <div className="p-4 bg-teal-accent/5 border border-teal-accent/20 rounded-lg">
          <div className="text-xs font-medium text-teal-dark uppercase tracking-widest mb-2">Main argument</div>
          <p className="text-sm font-medium text-ink leading-relaxed">{mainArg}</p>
        </div>
      )}
      {data.supporting_arguments?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Supporting arguments</div>
          <ul className="space-y-2">
            {data.supporting_arguments.map((a, i) => {
              const text = safeStr(a)
              if (!text) return null
              return (
                <li key={i} className="flex items-start gap-2 text-sm text-slate leading-relaxed">
                  <span className="text-teal-dark flex-shrink-0 mt-0.5">→</span><span>{text}</span>
                </li>
              )
            })}
          </ul>
        </div>
      )}
      {data.appeal_type && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">Appeal type</div>
          <span className="text-xs bg-teal-accent/10 text-teal-dark px-3 py-1.5 rounded-lg font-medium">{safeStr(data.appeal_type)}</span>
        </div>
      )}
      <TagList label="Influence techniques" items={data.influence_techniques} />
      {data.recommended_actions?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">Recommended actions</div>
          <div className="space-y-3">
            {data.recommended_actions.map((a, i) => (
              <div key={i} className="flex items-start gap-3 p-3 bg-surface rounded-lg">
                <div className="w-6 h-6 rounded-full bg-teal-dark text-white flex items-center justify-center text-xs font-medium flex-shrink-0">{i + 1}</div>
                <div className="flex-1">
                  <div className="text-sm font-medium text-ink mb-1">{safeStr(typeof a === 'string' ? a : a.action || a)}</div>
                  {typeof a === 'object' && a.channel && (
                    <div className="flex gap-2 flex-wrap">
                      <span className="text-xs bg-teal-accent/10 text-teal-dark px-2 py-0.5 rounded">{safeStr(a.channel)}</span>
                      {a.timing && <span className="text-xs bg-gray-100 text-slate px-2 py-0.5 rounded">{safeStr(a.timing)}</span>}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </Section>
  )
}

function AssessmentSection({ data }) {
  if (!data) return null
  return (
    <Section number="7" title="Assessment">
      {data.baseline_behavior && <Field label="Baseline behavior" value={data.baseline_behavior} />}
      {data.target_behavior && <Field label="Target behavior" value={data.target_behavior} />}
      {(data.refined_assessment_question || data.initial_assessment_question) && (
        <Field label="Assessment question" value={data.refined_assessment_question || data.initial_assessment_question} />
      )}
      {data.metrics?.length > 0 && (
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">Success metrics</div>
          <div className="space-y-3">
            {data.metrics.map((m, i) => (
              <div key={i} className="border border-gray-100 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2 flex-wrap">
                  <span className="text-sm font-medium text-ink">{typeof m === 'string' ? m : m.metric_name || `Metric ${i + 1}`}</span>
                  {m.metric_type && <span className="text-xs bg-gray-100 text-slate px-2 py-0.5 rounded">{m.metric_type}</span>}
                </div>
                {m.definition && <p className="text-xs text-slate leading-relaxed mb-2">{m.definition}</p>}
                {m.success_threshold && (
                  <div className="text-xs text-teal-dark font-medium bg-teal-accent/5 px-3 py-1.5 rounded">
                    Target: {m.success_threshold}
                  </div>
                )}
                {m.measurement_method && <p className="text-xs text-slate mt-2 leading-relaxed">Method: {m.measurement_method}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
    </Section>
  )
}

export function TARDetail({ source = 'examples' }) {
  const { slug, tarId, sessionId } = useParams()
  const [tar, setTar] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetch = source === 'session'
      ? sessionsApi.getTar(sessionId, tarId)
      : examplesApi.getTar(slug, tarId)
    fetch
      .then(r => setTar(r.data))
      .catch(e => { console.error(e); setError('Could not load this report.') })
      .finally(() => setLoading(false))
  }, [slug, tarId, sessionId, source])

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: '#F8F7F4' }}>
      <div className="flex items-center gap-2 text-sm text-slate">
        <div className="w-4 h-4 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin" />
        Loading report...
      </div>
    </div>
  )

  if (error || !tar) return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: '#F8F7F4' }}>
      <div className="text-sm text-slate">{error || 'Report not found'}</div>
    </div>
  )

  const audienceName = tar.audience_name || tar.header?.target_audience?.definition || tar.ta_id

  return (
    <div className="min-h-screen" style={{ background: '#F8F7F4' }}>
      <div style={{ background: 'radial-gradient(ellipse at 50% -20%, #102847 0%, #0A1628 60%)' }} className="px-6 py-10">
        <div className="max-w-4xl mx-auto">
          <Link to={source === 'session' ? `/session/${sessionId}` : `/examples/${slug}`}
            className="inline-flex items-center gap-1.5 text-xs text-white/40 hover:text-white/70 transition-colors mb-6">
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
              <path d="M8 2L4 6l4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
            {source === 'session' ? 'Back to analysis' : `Back to ${slug}`}
          </Link>
          <div className="text-xs font-medium text-teal-accent/60 uppercase tracking-widest mb-2">
            Target Audience Report · {tar.sobj_id}
          </div>
          <h1 className="text-2xl sm:text-3xl font-medium text-white/95 tracking-tight mb-2">{audienceName}</h1>
          <p className="text-sm text-white/45">{tar.sobj_statement}</p>
        </div>
      </div>
      <div className="max-w-4xl mx-auto px-6 py-8">
        <EffectivenessSection data={tar.effectiveness} />
        <ConditionsSection data={tar.conditions} />
        <VulnerabilitiesSection data={tar.vulnerabilities} />
        <SusceptibilitySection data={tar.susceptibility} />
        <AccessibilitySection data={tar.accessibility} />
        <NarrativeSection data={tar.narrative_and_actions} />
        <AssessmentSection data={tar.assessment} />
      </div>
    </div>
  )
}
