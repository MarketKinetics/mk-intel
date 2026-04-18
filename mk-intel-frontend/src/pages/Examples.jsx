import { useState, useEffect } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { examples } from '../api/client'

const PIPELINE_STEPS = [
  {
    label: 'Data ingestion',
    tag: 'Input',
    desc: 'Customer CSV uploaded and normalized against the canonical behavioral schema. 50+ field types mapped automatically across demographic, behavioral, and geographic dimensions.',
    data: {
      type: 'ingestion',
      label: 'Sample record — GlobalCart customer',
      content: [
        { field: 'customer_id', value: 'GC-00042', type: 'identifier' },
        { field: 'age', value: '38', type: 'demographic' },
        { field: 'subscription_status', value: 'active', type: 'behavioral' },
        { field: 'total_purchases', value: '14', type: 'behavioral' },
        { field: 'email_open_rate', value: '0.21', type: 'behavioral' },
        { field: 'churn_risk_score', value: '0.38', type: 'behavioral' },
        { field: 'zip_code', value: '90210', type: 'geographic' },
      ]
    }
  },
  {
    label: 'Coverage scoring',
    tag: 'Analysis',
    desc: 'Each record is scored for behavioral feature completeness. Only records with sufficient U.S. demographic signals are eligible for archetype matching — international or incomplete records are processed separately.',
    data: {
      type: 'coverage',
      label: 'Coverage output — GlobalCart',
      total: 50000,
      usMatched: 16490,
      nonUsCustomers: 32616,
      insufficientData: 894,
    }
  },
  {
    label: 'BTA matching',
    tag: 'Matching',
    desc: 'Eligible customers are mapped to 7 societal archetypes via structural matching against the census-derived baseline. Matches are made on age, income, tenure, education, and employment.',
    data: {
      type: 'archetypes',
      label: 'Archetype distribution — GlobalCart',
      archetypes: [
        { name: 'Diverse Mid-Life Workers', count: 7633 },
        { name: 'Young Hispanic Working Adults', count: 5349 },
        { name: 'Young Non-Owning Singles', count: 4234 },
        { name: 'Mid-Career Homeowners', count: 3309 },
        { name: 'Established Homeowners', count: 1127 },
      ]
    }
  },
  {
    label: 'ZIP enrichment',
    tag: 'Validation',
    desc: 'Geographic validation against ZCTA census data cross-checks the archetype assignment for each segment. Each segment receives a confidence case that determines how its profile is used downstream.',
    data: {
      type: 'cases',
      label: 'Confidence cases — GlobalCart',
      cases: [
        {
          case: 'Case A',
          label: 'Full alignment',
          detail: 'Census data confirms the archetype assignment across all dimensions. High confidence — profile used as-is.',
          count: '22,026',
          color: '#0D7377',
          bg: 'rgba(13,115,119,0.08)',
        },
        {
          case: 'Case B1',
          label: 'Income divergence',
          detail: 'Age and race align with census but income differs. Income descriptors adjusted to reflect ZIP-level data.',
          count: '0',
          color: '#14C9B8',
          bg: 'rgba(20,201,184,0.08)',
        },
        {
          case: 'Case B2',
          label: 'Race divergence',
          detail: 'Age and income align but racial composition differs. Cultural and media layer adjusted accordingly.',
          count: '0',
          color: '#5C6B7A',
          bg: 'rgba(92,107,122,0.08)',
        },
        {
          case: 'Case C',
          label: 'Full conflict',
          detail: 'Census data conflicts with the archetype on multiple dimensions. A custom archetype is built for this segment and a confidence penalty is applied.',
          count: '0',
          color: '#B4B2A9',
          bg: 'rgba(180,178,169,0.08)',
        },
      ]
    }
  },
  {
    label: 'Pre-filter',
    tag: 'Selection',
    desc: 'MK Intel refines each segment profile — adjusting names and behavioral descriptors to reflect both company-specific patterns and population-level psychological and media traits — then selects the most viable candidates per campaign objective.',
    data: {
      type: 'prefilter',
      label: 'Profile refinement + candidate selection — GlobalCart',
      candidates: [
        { id: 'CS00_BTA_05', original: 'Young Non-Owning Singles', refined: 'Mobile-First Value-Conscious Renewers', passed: true },
        { id: 'CS01_BTA_04', original: 'Mid-Career Homeowners', refined: 'Convenience-Seeking Homeowner Subscribers', passed: true },
        { id: 'CS01_BTA_05', original: 'Young Non-Owning Singles (alt.)', refined: 'Budget-Conscious Young Renters', passed: true },
        { id: 'CS01_BTA_06', original: 'Established Homeowners', refined: 'Financially Disciplined Mid-Career Homeowners', passed: true },
        { id: 'CS00_BTA_01', original: 'Older Non-Partnered Adults', refined: null, passed: false },
        { id: 'CS00_BTA_02', original: 'Young Hispanic Working Adults', refined: null, passed: false },
        { id: 'CS00_BTA_03', original: 'Retired Renters', refined: null, passed: false },
      ]
    }
  },
  {
    label: 'TAR generation',
    tag: 'Generation',
    desc: 'An 8-section Target Audience Report is generated per candidate audience. Each section builds on the previous, ensuring a coherent, internally consistent report throughout.',
    data: {
      type: 'sections',
      label: 'Report structure — 8 sections per audience',
      left: ['Audience profile', 'Behavioral drivers', 'Media & channels', 'Messaging strategy'],
      right: ['Tactical recommendations', 'Risk factors', 'Confidence assessment', 'Executive summary'],
    }
  },
  {
    label: 'Scoring & ranking',
    tag: 'Output',
    desc: 'A 4-dimensional algorithm scores each audience on effectiveness, susceptibility, vulnerability, and accessibility. Scores are combined with a size modifier to produce the final ranking.',
    data: {
      type: 'rankings',
      label: 'Final rankings — GlobalCart · renew subscription',
      rankings: [
        { rank: 1, name: 'Convenience-Seeking Homeowner Subscribers', score: '0.757', badge: 'First priority', dim: [0.71, 0.63, 0.95, 0.95] },
        { rank: 2, name: 'Mobile-First Value-Conscious Renewers', score: '0.709', badge: 'High priority', dim: [0.76, 0.44, 0.91, 0.95] },
        { rank: 3, name: 'Financially Disciplined Mid-Career Homeowners', score: '0.693', badge: 'Medium priority', dim: [0.86, 0.42, 0.95, 1.00] },
        { rank: 4, name: 'Budget-Conscious Young Renters', score: '0.692', badge: 'Lower priority', dim: [0.46, 0.63, 0.95, 1.00] },
      ]
    }
  },
]

const FALLBACK_EXAMPLES = [
  {
    slug: 'globalcart',
    name: 'GlobalCart',
    sector: 'E-Commerce',
    description: 'Subscription e-commerce platform. 50,000 customers, campaign objective: reduce subscription churn.',
    ta_count: 7, tar_count: 4, sobj_count: 1, zip_enrichment: false,
  },
  {
    slug: 'cloudsync',
    name: 'CloudSync',
    sector: 'SaaS',
    description: 'B2B SaaS platform. 1,500 customers with ZIP enrichment — real-world confidence case variety across full alignment, income divergence, and race divergence scenarios. Objective: reduce subscription cancellations.',
    ta_count: 6, tar_count: 4, sobj_count: 2, zip_enrichment: true,
  },
]

const DIM_LABELS = ['Eff.', 'Susc.', 'Vuln.', 'Acc.']

function StepData({ step }) {
  const d = step.data
  if (!d) return null

  if (d.type === 'ingestion') return (
    <div className="mt-3 rounded-lg overflow-hidden border border-gray-100">
      <div className="bg-navy-900/5 px-3 py-1.5 text-xs font-medium text-slate border-b border-gray-100">{d.label}</div>
      <div className="font-mono text-xs">
        {d.content.map((r, i) => (
          <div key={r.field} className={`flex items-center gap-2 px-3 py-1.5 ${i % 2 === 0 ? 'bg-white' : 'bg-surface'}`}>
            <span className="text-slate w-32 flex-shrink-0 truncate">{r.field}</span>
            <span className="text-ink font-medium flex-1">{r.value}</span>
            <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0
              ${r.type === 'behavioral' ? 'bg-teal-accent/10 text-teal-dark' :
                r.type === 'demographic' ? 'bg-blue-50 text-blue-700' :
                r.type === 'geographic' ? 'bg-amber-50 text-amber-700' :
                'bg-gray-100 text-slate'}`}>
              {r.type}
            </span>
          </div>
        ))}
      </div>
    </div>
  )

  if (d.type === 'coverage') return (
    <div className="mt-3 space-y-2">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-2">{d.label}</div>
      <div className="bg-surface rounded-lg p-4 space-y-3">
        <div className="flex justify-between text-xs">
          <span className="text-slate">Total customers</span>
          <span className="font-medium text-ink">{d.total.toLocaleString()}</span>
        </div>
        <div className="space-y-1.5">
          <div className="flex justify-between text-xs">
            <span className="text-slate">Matched to archetypes</span>
            <span className="font-medium text-teal-dark">{d.usMatched.toLocaleString()} ({Math.round(d.usMatched/d.total*100)}%)</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-1.5">
            <div className="bg-teal-dark h-1.5 rounded-full" style={{ width: `${d.usMatched/d.total*100}%` }} />
          </div>
          <div className="text-xs text-slate">U.S. customers with sufficient demographic data</div>
        </div>
        <div className="space-y-1.5 pt-1 border-t border-gray-200">
          <div className="flex justify-between text-xs">
            <span className="text-slate">Non-U.S. customers</span>
            <span className="font-medium text-slate">{d.nonUsCustomers.toLocaleString()}</span>
          </div>
          <div className="flex justify-between text-xs">
            <span className="text-slate">Insufficient data</span>
            <span className="font-medium text-slate">{d.insufficientData.toLocaleString()}</span>
          </div>
          <div className="text-xs text-slate">Processed without archetype matching</div>
        </div>
      </div>
    </div>
  )

  if (d.type === 'archetypes') return (
    <div className="mt-3">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">{d.label}</div>
      <div className="flex flex-col gap-2">
        {d.archetypes.map(a => {
          const max = Math.max(...d.archetypes.map(x => x.count))
          return (
            <div key={a.name} className="flex items-center gap-3">
              <span className="text-xs text-slate w-44 flex-shrink-0 leading-tight">{a.name}</span>
              <div className="flex-1 bg-gray-100 rounded-full h-1.5 overflow-hidden">
                <div className="h-full bg-teal-dark rounded-full transition-all" style={{ width: `${a.count/max*100}%` }} />
              </div>
              <span className="text-xs font-medium text-ink w-14 text-right">{a.count.toLocaleString()}</span>
            </div>
          )
        })}
      </div>
    </div>
  )

  if (d.type === 'cases') return (
    <div className="mt-3">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">{d.label}</div>
      <div className="flex flex-col gap-2">
        {d.cases.map(c => (
          <div key={c.case} className="rounded-lg p-3" style={{ background: c.bg }}>
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: c.color }} />
                <span className="text-xs font-medium text-ink">{c.case}</span>
                <span className="text-xs text-slate">— {c.label}</span>
              </div>
              <span className="text-xs font-medium text-ink">{c.count}</span>
            </div>
            <p className="text-xs text-slate leading-relaxed pl-4">{c.detail}</p>
          </div>
        ))}
      </div>
    </div>
  )

  if (d.type === 'prefilter') return (
    <div className="mt-3">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">{d.label}</div>
      <div className="flex flex-col gap-1">
        {d.candidates.map(c => (
          <div key={c.id} className={`flex items-start gap-2.5 py-2 px-3 rounded-lg ${c.passed ? 'bg-teal-accent/5' : 'bg-gray-50'}`}>
            <div className={`w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 ${c.passed ? 'bg-teal-accent/20' : 'bg-gray-200'}`}>
              {c.passed
                ? <svg width="8" height="8" viewBox="0 0 8 8" fill="none"><path d="M1.5 4l2 2 3-3" stroke="#0D7377" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
                : <svg width="8" height="8" viewBox="0 0 8 8" fill="none"><path d="M2 2l4 4M6 2L2 6" stroke="#B4B2A9" strokeWidth="1.2" strokeLinecap="round"/></svg>
              }
            </div>
            <div className="flex-1 min-w-0">
              {c.passed ? (
                <>
                  <div className="text-xs text-slate line-through mb-0.5">{c.original}</div>
                  <div className="text-xs font-medium text-teal-dark">→ {c.refined}</div>
                </>
              ) : (
                <div className="text-xs text-slate line-through">{c.original}</div>
              )}
            </div>
            {c.passed && <span className="text-xs text-teal-dark font-medium flex-shrink-0">Selected</span>}
          </div>
        ))}
      </div>
    </div>
  )

  if (d.type === 'sections') return (
    <div className="mt-3">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">{d.label}</div>
      <div className="grid grid-cols-2 gap-1.5">
        <div className="flex flex-col gap-1.5">
          {d.left.map((s, i) => (
            <div key={s} className="flex items-center gap-2 bg-surface rounded-lg px-3 py-2">
              <span className="text-xs text-slate w-4">{i + 1}</span>
              <span className="text-xs text-ink">{s}</span>
            </div>
          ))}
        </div>
        <div className="flex flex-col gap-1.5">
          {d.right.map((s, i) => (
            <div key={s} className="flex items-center gap-2 bg-surface rounded-lg px-3 py-2">
              <span className="text-xs text-slate w-4">{i + 5}</span>
              <span className="text-xs text-ink">{s}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )

  if (d.type === 'rankings') return (
    <div className="mt-3">
      <div className="text-xs font-medium text-slate uppercase tracking-widest mb-3">{d.label}</div>
      <div className="flex flex-col gap-2 mb-3">
        {d.rankings.map(r => (
          <div key={r.rank} className={`rounded-lg p-3 border ${r.rank === 1 ? 'border-teal-accent/30 bg-teal-accent/5' : 'border-gray-100 bg-surface'}`}>
            <div className="flex items-center gap-2 mb-2">
              <span className={`text-xs font-medium w-5 ${r.rank === 1 ? 'text-teal-dark' : 'text-slate'}`}>#{r.rank}</span>
              <span className="text-xs font-medium text-ink flex-1 leading-tight">{r.name}</span>
              <span className="text-sm font-medium text-ink">{r.score}</span>
              <span className={`text-xs px-2 py-0.5 rounded font-medium flex-shrink-0
                ${r.rank === 1 ? 'bg-teal-accent/15 text-teal-dark' :
                  r.rank === 2 ? 'bg-green-50 text-green-700' :
                  r.rank === 3 ? 'bg-amber-50 text-amber-700' :
                  'bg-gray-100 text-slate'}`}>
                {r.badge}
              </span>
            </div>
            <div className="flex gap-2 pl-5">
              {r.dim.map((v, i) => (
                <div key={i} className="flex-1">
                  <div className="text-xs text-slate mb-0.5">{DIM_LABELS[i]}</div>
                  <div className="w-full bg-gray-200 rounded-full h-1">
                    <div className={`h-1 rounded-full ${r.rank === 1 ? 'bg-teal-dark' : 'bg-gray-400'}`} style={{ width: `${v * 100}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <Link to="/examples/globalcart"
        className="flex items-center justify-center gap-2 w-full border border-teal-accent/30 text-teal-dark text-xs font-medium py-2.5 rounded-lg hover:bg-teal-accent/5 transition-colors">
        View full GlobalCart analysis
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 6h8M6 2l4 4-4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
      </Link>
    </div>
  )

  return null
}

function PipelineSidebar() {
  const [active, setActive] = useState(null)
  return (
    <div className="bg-white border border-gray-200/60 rounded-xl overflow-hidden sticky top-4">
      <div className="px-5 py-4 border-b border-gray-100">
        <div className="text-xs font-medium text-slate uppercase tracking-widest">The pipeline</div>
        <div className="text-xs text-slate mt-0.5">Click any step to see real data</div>
      </div>
      <div className="relative p-2">
        <div className="absolute left-6 top-2 bottom-2 w-px bg-gray-100" />
        {PIPELINE_STEPS.map((step, i) => (
          <button key={step.label} onClick={() => setActive(active === i ? null : i)}
            className="relative flex items-start gap-3 w-full p-2.5 rounded-lg hover:bg-surface transition-colors text-left group">
            <div className={`relative z-10 w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 text-xs font-medium transition-all
              ${active === i ? 'bg-teal-dark text-white' : 'bg-white border border-gray-200 text-slate group-hover:border-teal-dark/40'}`}>
              {i + 1}
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <span className={`text-sm font-medium transition-colors ${active === i ? 'text-teal-dark' : 'text-ink'}`}>{step.label}</span>
                <span className="text-xs text-slate bg-gray-100 px-1.5 py-0.5 rounded">{step.tag}</span>
              </div>
              {active === i && (
                <div className="mt-1.5">
                  <p className="text-xs text-slate leading-relaxed">{step.desc}</p>
                  <StepData step={step} />
                </div>
              )}
            </div>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"
              className={`flex-shrink-0 mt-1.5 transition-transform duration-200 flex-shrink-0 ${active === i ? 'rotate-180' : ''}`}>
              <path d="M3 5l4 4 4-4" stroke="#5C6B7A" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </button>
        ))}
      </div>
    </div>
  )
}

function HowItWorksTab() {
  const [step, setStep] = useState(0)
  const current = PIPELINE_STEPS[step]

  return (
    <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
      <div className="lg:col-span-2">
        <div className="bg-white border border-gray-200/60 rounded-xl overflow-hidden sticky top-4">
          <div className="px-4 py-3 border-b border-gray-100">
            <div className="text-xs font-medium text-slate uppercase tracking-widest">Pipeline steps</div>
          </div>
          <div className="p-2">
            {PIPELINE_STEPS.map((s, i) => (
              <button key={s.label} onClick={() => setStep(i)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-left transition-all mb-0.5
                  ${step === i ? 'bg-teal-accent/8 border border-teal-accent/20' : 'hover:bg-surface border border-transparent'}`}>
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-medium flex-shrink-0 transition-all
                  ${i < step ? 'bg-teal-accent/20 text-teal-dark' : step === i ? 'bg-teal-dark text-white' : 'bg-gray-100 text-slate'}`}>
                  {i < step ? '✓' : i + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <div className={`text-sm font-medium truncate ${step === i ? 'text-teal-dark' : 'text-ink'}`}>{s.label}</div>
                  <div className="text-xs text-slate">{s.tag}</div>
                </div>
                {step === i && (
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className="flex-shrink-0">
                    <path d="M5 3l4 4-4 4" stroke="#0D7377" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                )}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="lg:col-span-3">
        <div className="bg-white border border-gray-200/60 rounded-xl p-6">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-9 h-9 rounded-full bg-teal-dark text-white flex items-center justify-center text-sm font-medium flex-shrink-0">
              {step + 1}
            </div>
            <div>
              <div className="text-base font-medium text-ink">{current.label}</div>
              <div className="text-xs text-teal-dark font-medium">{current.tag}</div>
            </div>
            <div className="ml-auto text-xs text-slate">{step + 1} of {PIPELINE_STEPS.length}</div>
          </div>
          <p className="text-sm text-slate leading-relaxed border-b border-gray-100 pb-4 mb-4">{current.desc}</p>
          <StepData step={current} />
          <div className="flex gap-3 mt-6 pt-4 border-t border-gray-100">
            <button onClick={() => setStep(Math.max(0, step - 1))} disabled={step === 0}
              className="flex items-center gap-1.5 text-xs text-slate px-4 py-2 rounded-lg border border-gray-200 hover:border-gray-300 disabled:opacity-30 transition-all">
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M8 2L4 6l4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
              Previous
            </button>
            <button onClick={() => setStep(Math.min(PIPELINE_STEPS.length - 1, step + 1))} disabled={step === PIPELINE_STEPS.length - 1}
              className="flex items-center gap-1.5 text-xs font-medium text-white bg-teal-dark px-5 py-2 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all">
              Next step
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M4 2l4 4-4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

const PRESETS = {
  globalcart: {
    company: { name: 'GlobalCart', industry: 'E-Commerce', customer_type: 'B2C', description: 'Subscription e-commerce platform. 50,000 customers.' },
    objective: { statement: 'Reduce subscription churn' },
    sobjs: [{ id: 'SOBJ-01', statement: 'TA renews subscription at next billing cycle', direction: 'retain' }],
    csvUrl: 'https://raw.githubusercontent.com/MarketKinetics/mk-intel/main/data/demo/globalcart_ecommerce.csv',
    csvName: 'globalcart_ecommerce.csv',
  },
  cloudsync: {
    company: { name: 'CloudSync', industry: 'SaaS', customer_type: 'B2B', description: 'B2B SaaS platform. 1,500 customers.' },
    objective: { statement: 'Reduce subscription cancellations' },
    sobjs: [{ id: 'SOBJ-01', statement: 'TA renews subscription at next billing cycle', direction: 'retain' }],
    csvUrl: 'https://raw.githubusercontent.com/MarketKinetics/mk-intel/main/data/demo/cloudsync_saas.csv',
    csvName: 'cloudsync_saas.csv',
  },
}

function RunLiveTab() {
  const [selected, setSelected] = useState('globalcart')
  const navigate = useNavigate()

  function handleLaunch() {
    const preset = PRESETS[selected]
    navigate('/setup', { state: { preset } })
  }

  return (
    <div className="max-w-lg mx-auto">
      <div className="bg-white border border-gray-200/60 rounded-xl p-6 text-center">
        <div className="w-12 h-12 rounded-full bg-teal-accent/10 flex items-center justify-center mx-auto mb-4">
          <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
            <circle cx="11" cy="11" r="9" stroke="#0D7377" strokeWidth="1.3"/>
            <path d="M8 7.5l7 3.5-7 3.5V7.5z" fill="#0D7377"/>
          </svg>
        </div>
        <h3 className="text-base font-medium text-ink mb-2">Run a live analysis</h3>
        <p className="text-xs text-slate leading-relaxed mb-6 max-w-sm mx-auto">
          Launch the full MK Intel pipeline on a pre-loaded dataset. Uses real AI to generate audience reports.
        </p>
        <div className="flex gap-2 mb-5">
          {FALLBACK_EXAMPLES.map(ex => (
            <button key={ex.slug} onClick={() => setSelected(ex.slug)}
              className={`flex-1 p-3 rounded-lg border text-left transition-all
                ${selected === ex.slug ? 'border-teal-dark bg-teal-accent/5' : 'border-gray-200 hover:border-gray-300'}`}>
              <div className="text-xs font-medium text-ink mb-0.5">{ex.name}</div>
              <div className="text-xs text-slate">{ex.sector} · {ex.ta_count} segments</div>
            </button>
          ))}
        </div>
        <div className="bg-surface rounded-lg p-3 mb-5 text-left">
          <div className="text-xs text-slate mb-1">Selected dataset</div>
          <div className="text-xs text-ink leading-relaxed">
            {FALLBACK_EXAMPLES.find(e => e.slug === selected)?.description}
          </div>
        </div>
        <button onClick={handleLaunch} className="w-full bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 transition-all mb-3">
          Launch live analysis →
        </button>
        <div className="text-xs text-slate">
          Or <Link to="/setup" className="text-teal-dark font-medium hover:underline">upload your own data →</Link>
        </div>
      </div>
    </div>
  )
}

export function Examples() {
  const [tab, setTab] = useState('how')
  const [exampleList, setExampleList] = useState(FALLBACK_EXAMPLES)

  useEffect(() => {
    examples.list()
      .then(r => { if (r.data?.length) setExampleList(r.data) })
      .catch(() => {})
  }, [])

  const TABS = [
    { id: 'how', label: 'How it works' },
    { id: 'browse', label: 'Browse results' },
    { id: 'live', label: 'Run live' },
  ]

  return (
    <div className="min-h-screen" style={{ background: '#F8F7F4' }}>
      <div style={{ background: 'radial-gradient(ellipse at 50% -20%, #102847 0%, #0A1628 60%)' }}
        className="px-4 sm:px-6 py-10 sm:py-14">
        <div className="max-w-6xl mx-auto">
          <div className="text-xs font-medium text-teal-accent/60 uppercase tracking-widest mb-3">Live examples</div>
          <h1 className="text-2xl sm:text-3xl font-medium text-white/95 tracking-tight mb-3">
            See MK Intel in action
          </h1>
          <p className="text-sm text-white/45 leading-relaxed max-w-xl mb-8">
            Browse completed analyses, walk through the pipeline step by step, or launch a live demo. No account needed.
          </p>
          <div className="flex gap-1 bg-white/8 border border-white/10 rounded-xl p-1 w-fit flex-wrap">
            {TABS.map(t => (
              <button key={t.id} onClick={() => setTab(t.id)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-all
                  ${tab === t.id ? 'bg-white text-ink' : 'text-white/50 hover:text-white/80'}`}>
                {t.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-8 sm:py-10">
        {tab === 'browse' && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            {exampleList.map(ex => (
              <Link key={ex.slug} to={`/examples/${ex.slug}`}
                className="bg-white border border-gray-200/80 rounded-xl p-6 hover:border-teal-dark/30 hover:shadow-sm transition-all group block">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-base font-medium text-ink">{ex.name}</span>
                  <span className="text-xs text-teal-dark bg-teal-accent/10 px-2 py-0.5 rounded font-medium">{ex.sector}</span>
                </div>
                <p className="text-xs text-slate leading-relaxed mb-4">{ex.description}</p>
                <div className="flex gap-4 mb-4 pt-3 border-t border-gray-100">
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
                <div className="flex items-center gap-1 text-xs text-teal-dark font-medium opacity-0 group-hover:opacity-100 transition-opacity">
                  View rankings, reports & summaries
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 6h8M6 2l4 4-4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/></svg>
                </div>
              </Link>
            ))}
          </div>
        )}
                {tab === 'how' && <HowItWorksTab />}
        {tab === 'live' && <RunLiveTab />}
      </div>
    </div>
  )
}
