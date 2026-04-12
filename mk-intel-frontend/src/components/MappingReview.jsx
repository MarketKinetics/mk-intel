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

const TABS = [
  { id: 'review',    label: 'Needs review' },
  { id: 'confirmed', label: 'Confirmed' },
  { id: 'skipped',   label: 'Unmatched / skipped' },
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
        <div className="flex-shrink-0 w-52">
          <div className="text-xs text-slate mb-1">Maps to</div>
          <select
            className="w-full border border-gray-200 rounded-lg px-3 py-2 text-xs text-ink focus:outline-none focus:border-teal-dark bg-white transition-colors"
            value={info.canonical_field || ''}
            onChange={e => onChange(col, e.target.value || null)}>
            <option value="">— Skip this field —</option>
            {canonicalFields.map(f => (
              <option key={f} value={f}>{f}</option>
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
        {TABS.map(tab => (
          <button key={tab.id} onClick={() => setActiveTab(tab.id)}
            className={`px-3 py-2 text-xs font-medium transition-all border-b-2 -mb-px
              ${activeTab === tab.id
                ? 'text-teal-dark border-teal-dark'
                : 'text-slate border-transparent hover:text-ink'}`}>
            {tab.label}
            <span className="ml-1.5 bg-gray-100 text-slate px-1.5 py-0.5 rounded-full text-xs">
              {tabCounts[tab.id]}
            </span>
          </button>
        ))}
      </div>

      <div className="space-y-2 mb-6 max-h-80 overflow-y-auto pr-1">
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
