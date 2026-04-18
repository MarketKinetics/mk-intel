import { useState, useEffect } from 'react'
import { sessions } from '../api/client'

const METHOD_CONFIG = {
  rules:          { label: 'Auto-matched',  bg: 'bg-green-50',  border: 'border-green-200', text: 'text-green-700' },
  llm:            { label: 'AI-matched',    bg: 'bg-blue-50',   border: 'border-blue-200',  text: 'text-blue-700' },
  user_amended:   { label: 'Amended',       bg: 'bg-teal-accent/5', border: 'border-teal-dark/20', text: 'text-teal-dark' },
  user_skipped:   { label: 'Skipped',       bg: 'bg-gray-50',   border: 'border-gray-200',  text: 'text-slate' },
  unmatched:      { label: 'Unmatched',     bg: 'bg-red-50',    border: 'border-red-100',   text: 'text-red-600' },
  collision_loser:{ label: 'Collision',     bg: 'bg-orange-50', border: 'border-orange-200',text: 'text-orange-600' },
}

// Expected value format hints shown in the dropdown next to each canonical field
const FIELD_TYPE_HINTS = {
  customer_id: 'string', age: 'integer e.g. 34', age_bin: 'string e.g. 25-34',
  gender: 'string e.g. Male/Female', income_annual: 'float e.g. 75000',
  income_tier: 'string e.g. 50-99k', education: 'string e.g. Bachelor',
  marital_status: 'string e.g. Married', housing_tenure: 'string e.g. Owner',
  zip_code: 'string e.g. 90210', country: 'string e.g. US',
  customer_since: 'date e.g. 2022-01-15',
  sessions_last_7d: 'integer e.g. 3', sessions_last_30d: 'integer e.g. 12',
  sessions_last_90d: 'integer e.g. 35', last_active_date: 'date e.g. 2024-03-01',
  days_since_active: 'integer e.g. 14', feature_adoption_count: 'integer e.g. 5',
  nps_score: 'integer 0-10', support_tickets_total: 'integer e.g. 2',
  support_tickets_90d: 'integer e.g. 1', cancellation_attempts: 'integer e.g. 0',
  subscription_plan: 'string e.g. Monthly/Annual', subscription_status: 'string e.g. active/cancelled',
  mrr: 'float e.g. 49.99', arr: 'float e.g. 599.88', ltv: 'float e.g. 1200',
  total_purchases: 'integer e.g. 24', purchases_last_30d: 'integer e.g. 3',
  purchases_last_90d: 'integer e.g. 8', avg_order_value: 'float e.g. 45.50',
  last_purchase_date: 'date e.g. 2024-03-10', days_since_purchase: 'integer e.g. 7',
  payment_failures_total: 'integer e.g. 0', discount_usage_pct: 'float 0-1 e.g. 0.35',
  cart_abandonment_rate: 'float 0-1 e.g. 0.20', return_rate: 'float 0-1 e.g. 0.05',
  churn_risk_score: 'float 0-1 e.g. 0.72', churn_risk_tier: 'string e.g. high/medium/low',
  days_to_renewal: 'integer e.g. 30', renewal_date: 'date e.g. 2024-06-01',
  onboarding_completed: 'boolean true/false', onboarding_completion_pct: 'float 0-1 e.g. 0.80',
  lifecycle_stage: 'string e.g. active/at_risk', upgrades_total: 'integer e.g. 1',
  downgrades_total: 'integer e.g. 0', referrals_made: 'integer e.g. 2',
  email_open_rate: 'float 0-1 e.g. 0.25', email_click_rate: 'float 0-1 e.g. 0.08',
  push_opt_in: 'boolean true/false', sms_opt_in: 'boolean true/false',
  preferred_channel: 'string e.g. email/mobile/sms', avg_review_score: 'float 1-5 e.g. 4.2',
  nps_tier: 'string e.g. promoter/passive/detractor', sentiment_score: 'float -1 to 1',
  pain_points: 'text or list', source_count: 'integer e.g. 3',
}

// Only show tabs that have fields — hide Confirmed tab by default (Fix 4)
const ALL_TABS = [
  { id: 'review',    label: 'Needs review' },
  { id: 'skipped',   label: 'Unmatched / skipped' },
  { id: 'confirmed', label: 'Confirmed' },
]

function FieldRow({ col, info, samples, canonicalFields, onChange }) {
  const method = info.method || 'unmatched'
  const config = METHOD_CONFIG[method] || METHOD_CONFIG.unmatched
  const sampleVals = (samples[col] || []).slice(0, 4)

  return (
    <div className={`border rounded-lg p-4 ${config.bg} ${config.border}`}>
      <div className="flex items-start gap-4 flex-wrap">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1.5 flex-wrap">
            <span className="text-sm font-medium text-ink font-mono">{col}</span>
            <span className={`text-xs px-2 py-0.5 rounded-full font-medium border ${config.bg} ${config.text} ${config.border}`}>
              {config.label}
            </span>
            {info.collision_note && (
              <span className="text-xs text-orange-500 italic">{info.collision_note}</span>
            )}
          </div>
          {sampleVals.length > 0 && (
            <div className="flex gap-1 flex-wrap">
              {sampleVals.map((v, i) => (
                <span key={i} className="text-xs bg-white/80 border border-gray-200 px-1.5 py-0.5 rounded text-slate font-mono">
                  {String(v).length > 18 ? String(v).slice(0, 18) + '…' : String(v)}
                </span>
              ))}
            </div>
          )}
        </div>
        <div className="flex-shrink-0 w-56">
          <div className="text-xs text-slate mb-1">Maps to</div>
          <select
            className="w-full border border-gray-200 rounded-lg px-3 py-2 text-xs text-ink focus:outline-none focus:border-teal-dark bg-white transition-colors"
            value={info.canonical_field || ''}
            onChange={e => onChange(col, e.target.value || null)}>
            <option value="">— Skip this field —</option>
            {canonicalFields.map(f => (
              <option key={f} value={f}>
                {f}{FIELD_TYPE_HINTS[f] ? ` · ${FIELD_TYPE_HINTS[f]}` : ''}
              </option>
            ))}
          </select>
        </div>
      </div>
    </div>
  )
}

export function MappingReview({ sessionId, onConfirm }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [amendments, setAmendments] = useState({})
  const [activeTab, setActiveTab] = useState('review')
  const [mappings, setMappings] = useState({})

  useEffect(() => {
    sessions.getColumnMapping(sessionId)
      .then(r => {
        setData(r.data)
        setMappings(r.data.mapping?.mappings || {})
        setLoading(false)
      })
      .catch(() => {
        setError('Could not load column mapping.')
        setLoading(false)
      })
  }, [sessionId])

  function handleChange(col, canonicalField) {
    const newMappings = {
      ...mappings,
      [col]: {
        ...mappings[col],
        canonical_field: canonicalField || null,
        confidence: 'user',
        method: canonicalField ? 'user_amended' : 'user_skipped',
      }
    }
    setMappings(newMappings)
    setAmendments(prev => ({ ...prev, [col]: canonicalField || null }))
  }

  async function handleConfirm() {
    setSaving(true)
    try {
      if (Object.keys(amendments).length > 0) {
        await sessions.updateColumnMapping(sessionId, amendments)
      }
      onConfirm()
    } catch (e) {
      setError('Failed to save mapping. Please try again.')
      setSaving(false)
    }
  }

  if (loading) return (
    <div className="flex items-center justify-center py-12">
      <div className="flex items-center gap-2 text-sm text-slate">
        <div className="w-4 h-4 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin" />
        Loading column mapping...
      </div>
    </div>
  )

  if (error && !data) return (
    <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm text-red-700">{error}</div>
  )

  const canonicalFields = data?.canonical_fields || []
  const samples = data?.samples || {}

  // Categorize fields
  const reviewFields = []
  const confirmedFields = []
  const skippedFields = []

  Object.entries(mappings).forEach(([col, info]) => {
    const method = info.method || 'unmatched'
    if (method === 'user_skipped' || method === 'unmatched' || method === 'collision_loser') {
      skippedFields.push([col, info])
    } else if (method === 'llm' || method === 'user_amended') {
      reviewFields.push([col, info])
    } else {
      confirmedFields.push([col, info])
    }
  })

  const tabCounts = {
    review: reviewFields.length,
    confirmed: confirmedFields.length,
    skipped: skippedFields.length,
  }

  const activeFields = activeTab === 'review' ? reviewFields
    : activeTab === 'confirmed' ? confirmedFields
    : skippedFields

  const amendedCount = Object.keys(amendments).length

  return (
    <div>
      <div className="mb-5">
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1">Column mapping review</div>
        <p className="text-xs text-slate leading-relaxed">
          Review how your dataset columns were mapped to MK Intel's schema.
          Fix any incorrect mappings or assign unmatched fields before running the analysis.
        </p>
      </div>

      <div className="grid grid-cols-3 gap-3 mb-5">
        <div className="bg-green-50 border border-green-200 rounded-lg p-3 text-center">
          <div className="text-lg font-medium text-green-700">{confirmedFields.length}</div>
          <div className="text-xs text-green-600">Confirmed</div>
        </div>
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 text-center">
          <div className="text-lg font-medium text-amber-700">{reviewFields.length}</div>
          <div className="text-xs text-amber-600">Needs review</div>
        </div>
        <div className="bg-red-50 border border-red-100 rounded-lg p-3 text-center">
          <div className="text-lg font-medium text-red-600">{skippedFields.length}</div>
          <div className="text-xs text-red-500">Unmatched</div>
        </div>
      </div>

      <div className="flex gap-1 border-b border-gray-200 mb-4">
        {ALL_TABS.map(tab => {
          const count = tab.id === 'review' ? reviewFields.length
            : tab.id === 'skipped' ? skippedFields.length
            : confirmedFields.length
          // Hide confirmed tab if it has no items that need attention
          if (tab.id === 'confirmed' && reviewFields.length > 0 && activeTab !== 'confirmed') return null
          return (
            <button key={tab.id} onClick={() => setActiveTab(tab.id)}
              className={`px-3 py-2 text-xs font-medium transition-all border-b-2 -mb-px
                ${activeTab === tab.id
                  ? 'text-teal-dark border-teal-dark'
                  : 'text-slate border-transparent hover:text-ink'}`}>
              {tab.label}
              <span className="ml-1.5 bg-gray-100 text-slate px-1.5 py-0.5 rounded-full text-xs">
                {count}
              </span>
            </button>
          )
        })}
      </div>

      <div className="space-y-2 mb-6 max-h-80 overflow-y-auto pr-1">
        {activeTab === 'review' && activeFields.length > 0 && (
          <p className="text-xs text-slate italic mb-2">
            These fields were mapped by AI — verify the mapping matches your data. The dropdown shows each field's expected format.
          </p>
        )}
        {activeTab === 'skipped' && activeFields.length > 0 && (
          <p className="text-xs text-slate italic mb-2">
            These fields could not be mapped. If relevant, assign them manually using the dropdown.
          </p>
        )}
        {activeFields.length === 0 ? (
          <div className="text-xs text-slate text-center py-6">No fields in this category</div>
        ) : (
          activeFields.map(([col, info]) => (
            <FieldRow
              key={col}
              col={col}
              info={info}
              samples={samples}
              canonicalFields={canonicalFields}
              onChange={handleChange}
            />
          ))
        )}
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-xs text-red-700 mb-4">{error}</div>
      )}

      <div className="flex gap-3">
        <button
          onClick={handleConfirm}
          disabled={saving}
          className="flex-1 bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all flex items-center justify-center gap-2">
          {saving ? (
            <>
              <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              Saving...
            </>
          ) : (
            <>{amendedCount > 0 ? `Confirm mapping (${amendedCount} changes)` : 'Confirm mapping'} →</>
          )}
        </button>
      </div>
      <p className="text-xs text-slate text-center mt-2">
        Unmatched fields will be stored separately and won't affect the analysis
      </p>
    </div>
  )
}
