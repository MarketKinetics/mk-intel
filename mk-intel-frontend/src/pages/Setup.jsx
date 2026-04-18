import { useState, useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { sessions } from '../api/client'
import { useDemo } from '../hooks/useDemo'

const INDUSTRIES = ['E-Commerce', 'SaaS', 'Retail', 'Banking', 'Media', 'Healthcare', 'Other']
const CUSTOMER_TYPES = ['B2C', 'B2B', 'B2B2C']
const SOBJ_DIRECTIONS = ['increase', 'decrease', 'retain', 'acquire']

function StepIndicator({ current, total }) {
  return (
    <div className="flex items-center gap-2 mb-8">
      {Array.from({ length: total }).map((_, i) => (
        <div key={i} className="flex items-center gap-2">
          <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-medium transition-all
            ${i < current ? 'bg-teal-dark text-white' :
              i === current ? 'bg-teal-dark text-white ring-2 ring-teal-accent/30 ring-offset-2' :
              'bg-gray-100 text-slate'}`}>
            {i < current ? '✓' : i + 1}
          </div>
          {i < total - 1 && (
            <div className={`w-8 h-px ${i < current ? 'bg-teal-dark' : 'bg-gray-200'}`} />
          )}
        </div>
      ))}
    </div>
  )
}

function Step1Company({ data, onChange, onNext, readOnly }) {
  const valid = data.name && data.industry && data.customer_type
  return (
    <div className="space-y-5">
      <div>
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Company name *</div>
        <input
          className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors"
          placeholder="e.g. Acme Corp"
          value={data.name}
          onChange={e => !readOnly && onChange({ ...data, name: e.target.value })}
          readOnly={readOnly}
        />
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Industry *</div>
          <select
            className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors bg-white"
            value={data.industry}
            onChange={e => onChange({ ...data, industry: e.target.value })}>
            <option value="">Select...</option>
            {INDUSTRIES.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>
        <div>
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Customer type *</div>
          <select
            className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors bg-white"
            value={data.customer_type}
            onChange={e => onChange({ ...data, customer_type: e.target.value })}>
            <option value="">Select...</option>
            {CUSTOMER_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
      </div>
      <div>
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Brief description <span className="text-slate normal-case font-normal">(optional)</span></div>
        <textarea
          className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors resize-none"
          rows={3}
          placeholder="What does your company do? What product are customers subscribed to?"
          value={data.description}
          onChange={e => !readOnly && onChange({ ...data, description: e.target.value })}
          readOnly={readOnly}
        />
      </div>
      <button onClick={onNext} disabled={!valid}
        className="w-full bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all">
        Continue →
      </button>
    </div>
  )
}

function Step2Objective({ data, onChange, onNext, onBack }) {
  const valid = data.statement
  return (
    <div className="space-y-5">
      <div>
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Campaign objective *</div>
        <p className="text-xs text-slate mb-3">What is the primary outcome you want to achieve with this campaign?</p>
        <textarea
          className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors resize-none"
          rows={3}
          placeholder="e.g. Reduce subscription churn among active customers"
          value={data.statement}
          onChange={e => onChange({ ...data, statement: e.target.value })}
        />
      </div>
      <div className="bg-surface rounded-lg p-4">
        <div className="text-xs font-medium text-ink mb-2">Examples</div>
        {[
          'Reduce subscription churn',
          'Increase plan upgrade conversions',
          'Reactivate cancelled accounts',
          'Drive first purchase from trial users',
        ].map(ex => (
          <button key={ex} onClick={() => onChange({ ...data, statement: ex })}
            className="block text-xs text-teal-dark hover:text-teal-dark/70 mb-1.5 text-left transition-colors">
            + {ex}
          </button>
        ))}
      </div>
      <div className="flex gap-3">
        <button onClick={onBack}
          className="flex-1 border border-gray-200 text-slate text-sm py-3 rounded-lg hover:border-gray-300 transition-all">
          ← Back
        </button>
        <button onClick={onNext} disabled={!valid}
          className="flex-1 bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all">
          Continue →
        </button>
      </div>
    </div>
  )
}

function Step3Sobjs({ data, onChange, onNext, onBack, readOnly }) {
  const sobjs = data.sobjs || [{ id: 'SOBJ-01', statement: '', direction: 'increase' }]

  function updateSobj(idx, field, val) {
    const updated = sobjs.map((s, i) => i === idx ? { ...s, [field]: val } : s)
    onChange({ ...data, sobjs: updated })
  }

  function addSobj() {
    if (sobjs.length >= 3) return
    onChange({ ...data, sobjs: [...sobjs, { id: `SOBJ-0${sobjs.length + 1}`, statement: '', direction: 'increase' }] })
  }

  function removeSobj(idx) {
    onChange({ ...data, sobjs: sobjs.filter((_, i) => i !== idx) })
  }

  const valid = sobjs.every(s => s.statement)

  return (
    <div className="space-y-5">
      <div>
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1">Supporting objectives</div>
        <p className="text-xs text-slate mb-4">Define the specific behavioral targets — what do you want the audience to do?</p>
        <div className="space-y-4">
          {sobjs.map((sobj, idx) => (
            <div key={idx} className="border border-gray-200 rounded-lg p-4">
              <div className="flex items-center justify-between mb-3">
                <span className="text-xs font-medium text-slate uppercase tracking-widest">{sobj.id}</span>
                {idx > 0 && (
                  <button onClick={() => removeSobj(idx)} className="text-xs text-red-400 hover:text-red-600">Remove</button>
                )}
              </div>
              <div className="space-y-3">
                <div>
                  <div className="text-xs text-slate mb-1">Behavioral target</div>
                  <input
                    className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm text-ink focus:outline-none focus:border-teal-dark transition-colors"
                    placeholder="e.g. TA renews subscription at next billing cycle"
                    value={sobj.statement}
                    onChange={e => updateSobj(idx, 'statement', e.target.value)}
                  />
                </div>
                <div>
                  <div className="text-xs text-slate mb-1">Direction</div>
                  <select
                    className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm text-ink focus:outline-none focus:border-teal-dark bg-white transition-colors"
                    value={sobj.direction}
                    onChange={e => updateSobj(idx, 'direction', e.target.value)}>
                    {SOBJ_DIRECTIONS.map(d => <option key={d} value={d}>{d}</option>)}
                  </select>
                </div>
              </div>
            </div>
          ))}
        </div>
        {sobjs.length < 3 && (
          <button onClick={addSobj}
            className="mt-3 text-xs text-teal-dark font-medium hover:opacity-70 transition-opacity">
            + Add another objective
          </button>
        )}
      </div>
      <div className="flex gap-3">
        <button onClick={onBack}
          className="flex-1 border border-gray-200 text-slate text-sm py-3 rounded-lg hover:border-gray-300 transition-all">
          ← Back
        </button>
        <button onClick={onNext} disabled={!valid}
          className="flex-1 bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all">
          Continue →
        </button>
      </div>
    </div>
  )
}

function Step4Upload({ data, onBack, onSubmit, loading, error, byokKey, onByokChange, preset, presetCsvFile, presetCsvLoading, setPresetCsvFile, setPresetCsvLoading }) {
  const [file, setFile] = useState(null)
  const [dragging, setDragging] = useState(false)

  // On mount, fetch preset CSV if provided
  useEffect(() => {
    if (preset && !presetCsvFile && !presetCsvLoading) {
      setPresetCsvLoading(true)
      fetch(preset.csvUrl)
        .then(r => r.blob())
        .then(blob => {
          const f = new File([blob], preset.csvName, { type: 'text/csv' })
          setPresetCsvFile(f)
          setFile(f)
          setPresetCsvLoading(false)
        })
        .catch(() => setPresetCsvLoading(false))
    } else if (presetCsvFile) {
      setFile(presetCsvFile)
    }
  }, [])

  function handleDrop(e) {
    e.preventDefault()
    setDragging(false)
    const f = e.dataTransfer.files[0]
    if (f) setFile(f)
  }

  function handleFile(e) {
    const f = e.target.files[0]
    if (f) setFile(f)
  }

  return (
    <div className="space-y-5">
      <div>
        <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1.5">Upload customer data *</div>
        <p className="text-xs text-slate mb-4">CSV, XLSX, JSON, TSV or Parquet file with customer records. The platform maps your columns automatically.</p>

        {preset ? (
          <div className={`border-2 rounded-xl p-8 text-center transition-all ${presetCsvLoading ? 'border-gray-200' : 'border-teal-dark bg-teal-accent/5'}`}>
            {presetCsvLoading ? (
              <div className="flex items-center justify-center gap-2 text-sm text-slate">
                <div className="w-4 h-4 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin" />
                Loading dataset...
              </div>
            ) : (
              <div>
                <div className="text-sm font-medium text-teal-dark mb-1">{preset.csvName}</div>
                <div className="text-xs text-slate">Pre-loaded demo dataset · ready to run</div>
              </div>
            )}
          </div>
        ) : (
          <div
            onDragOver={e => { e.preventDefault(); setDragging(true) }}
            onDragLeave={() => setDragging(false)}
            onDrop={handleDrop}
            className={`border-2 border-dashed rounded-xl p-8 text-center transition-all cursor-pointer ${
              dragging ? 'border-teal-dark bg-teal-accent/5' :
              file ? 'border-teal-dark bg-teal-accent/5' :
              'border-gray-200 hover:border-gray-300'
            }`}
            onClick={() => document.getElementById('csv-input').click()}>
            <input id="csv-input" type="file" accept=".csv,.tsv,.txt,.json,.jsonl,.xlsx,.xls,.parquet" className="hidden" onChange={handleFile} />
            {file ? (
              <div>
                <div className="text-sm font-medium text-teal-dark mb-1">{file.name}</div>
                <div className="text-xs text-slate">{(file.size / 1024).toFixed(0)} KB · Click to replace</div>
              </div>
            ) : (
              <div>
                <svg width="32" height="32" viewBox="0 0 32 32" fill="none" className="mx-auto mb-3">
                  <path d="M16 4v16M8 12l8-8 8 8M6 24h20" stroke="#5C6B7A" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
                <div className="text-sm font-medium text-ink mb-1">Drop your CSV here</div>
                <div className="text-xs text-slate">or click to browse</div>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="bg-surface rounded-lg p-4">
        <div className="text-xs font-medium text-ink mb-2">What fields does the platform use?</div>
        <p className="text-xs text-slate leading-relaxed">Age, location, income, purchase history, subscription status, churn risk, engagement metrics. Column names are mapped automatically — your CSV doesn't need to match exactly.</p>
      </div>

      <div className="border border-gray-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="text-xs font-medium text-ink">Use your own Anthropic API key</div>
          <span className="text-xs text-slate bg-surface px-2 py-0.5 rounded">Optional · unlimited runs</span>
        </div>
        <p className="text-xs text-slate mb-3 leading-relaxed">By default you get 1 free analysis run. Enter your own key for unlimited runs — you pay only for what you use (~$0.15–0.20 per run).</p>
        <input
          className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm text-ink focus:outline-none focus:border-teal-dark font-mono transition-colors"
          placeholder="sk-ant-..."
          type="password"
          value={byokKey}
          onChange={e => onByokChange(e.target.value)}
        />
        {byokKey && (
          <div className="text-xs text-teal-dark mt-1.5">✓ Your key will be used — demo quota bypassed</div>
        )}
      </div>

      <div className="bg-amber-50 border border-amber-200/60 rounded-lg p-3">
        <div className="text-xs text-amber-700 font-medium mb-0.5">Data notice</div>
        <div className="text-xs text-amber-600 leading-relaxed">Uploaded data is retained privately for platform accuracy. Do not upload data containing sensitive personal information (SSN, passwords, financial account numbers).</div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-xs text-red-700">{error}</div>
      )}

      <div className="flex gap-3">
        <button onClick={onBack} disabled={loading}
          className="flex-1 border border-gray-200 text-slate text-sm py-3 rounded-lg hover:border-gray-300 disabled:opacity-40 transition-all">
          ← Back
        </button>
        <button onClick={() => file && onSubmit(file)} disabled={!file || loading}
          className="flex-1 bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 disabled:opacity-40 transition-all flex items-center justify-center gap-2">
          {loading ? (
            <>
              <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              Starting analysis...
            </>
          ) : 'Run analysis →'}
        </button>
      </div>
    </div>
  )
}

export function Setup() {
  const navigate = useNavigate()
  const location = useLocation()
  const preset = location.state?.preset || null
  const { token, requestToken, isExhausted, runsRemaining } = useDemo()
  const [step, setStep] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [recruiterCode, setRecruiterCode] = useState('')
  const [byokKey, setByokKey] = useState('')
  const [showRecruiterInput, setShowRecruiterInput] = useState(false)
  const [presetCsvFile, setPresetCsvFile] = useState(null)
  const [presetCsvLoading, setPresetCsvLoading] = useState(false)

  const [company, setCompany] = useState(preset?.company || { name: '', industry: '', customer_type: '', description: '' })
  const [objective, setObjective] = useState(preset?.objective || { statement: '' })
  const [sobjData, setSobjData] = useState({ sobjs: preset?.sobjs || [{ id: 'SOBJ-01', statement: '', direction: 'retain' }] })

  const STEPS = ['Company', 'Objective', 'Targets', 'Upload']

  async function ensureToken() {
    if (token) return token
    const data = await requestToken()
    return data.token
  }

  async function handleSubmit(file) {
    setLoading(true)
    setError(null)
    try {
      // If BYOK key provided, skip demo token entirely
      const demoToken = byokKey ? null : await ensureToken()

      // 1. Create session
      const sessionRes = await sessions.create()
      const sessionId = sessionRes.data.session_id

      // 2. Set company
      await sessions.setCompany(sessionId, {
        name: company.name,
        description_input: company.description,
        industry: company.industry,
        customer_type: company.customer_type,
      })

      // 3. Set objective
      await sessions.setObjective(sessionId, {
        id: 'OBJ-01',
        statement: objective.statement,
        verb: 'reduce',
        object: 'churn',
      })

      // 4. Add and approve SOBJs
      for (const sobj of sobjData.sobjs) {
        await sessions.addSobj(sessionId, {
          id: sobj.id,
          statement: sobj.statement,
          direction: sobj.direction,
        })
        await sessions.approveSobj(sessionId, sobj.id)
      }

      // 5. Ingest file
      const ingestRes = await sessions.ingest(sessionId, file)
      const ingestJobId = ingestRes.data.job_id

      // Navigate to processing page with all job info
      navigate(`/processing/${sessionId}`, {
        state: {
          ingestJobId,
          demoToken,
          byokKey,
          company: company.name,
        }
      })
    } catch (e) {
      const detail = e.response?.data?.detail
      if (e.response?.status === 402) {
        setError(`Demo quota exceeded. You've used all your free analysis runs.`)
      } else {
        setError(detail || 'Something went wrong. Please try again.')
      }
      setLoading(false)
    }
  }

  // Quota exhausted screen
  if (isExhausted) {
    return (
      <div className="min-h-screen flex items-center justify-center px-6" style={{ background: '#F8F7F4' }}>
        <div className="bg-white border border-gray-200/60 rounded-xl p-8 max-w-md w-full text-center">
          <div className="w-12 h-12 bg-amber-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <path d="M10 2l8 14H2L10 2z" stroke="#D97706" strokeWidth="1.3" strokeLinejoin="round"/>
              <path d="M10 8v4M10 14v.5" stroke="#D97706" strokeWidth="1.3" strokeLinecap="round"/>
            </svg>
          </div>
          <div className="text-base font-medium text-ink mb-2">Demo quota reached</div>
          <p className="text-sm text-slate mb-6 leading-relaxed">You've used all your free analysis runs. Browse the example analyses to explore the platform's output.</p>
          <div className="space-y-3">
            <button onClick={() => navigate('/examples')}
              className="w-full bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90 transition-opacity">
              Browse examples
            </button>
            {!showRecruiterInput ? (
              <button onClick={() => setShowRecruiterInput(true)}
                className="w-full border border-gray-200 text-slate text-sm py-3 rounded-lg hover:border-gray-300 transition-all">
                I have a magic key
              </button>
            ) : (
              <div className="space-y-2">
                <input
                  className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm text-ink focus:outline-none focus:border-teal-dark"
                  placeholder="Enter your magic key"
                  value={recruiterCode}
                  onChange={e => setRecruiterCode(e.target.value)}
                />
                <button
                  onClick={async () => {
                    try {
                      await requestToken(recruiterCode, '')
                      setShowRecruiterInput(false)
                    } catch (e) {
                      setError(e.response?.data?.detail || 'Invalid code')
                    }
                  }}
                  className="w-full bg-teal-dark text-white text-sm font-medium py-2.5 rounded-lg hover:opacity-90">
                  Apply key
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen" style={{ background: '#F8F7F4' }}>
      <div style={{ background: 'radial-gradient(ellipse at 50% -20%, #102847 0%, #0A1628 60%)' }}
        className="px-6 py-10">
        <div className="max-w-xl mx-auto">
          <div className="text-xs font-medium text-teal-accent/60 uppercase tracking-widest mb-3">New analysis</div>
          <h1 className="text-2xl font-medium text-white/95 tracking-tight mb-1">Set up your analysis</h1>
          <p className="text-sm text-white/45">Tell us about your company and campaign — we'll do the rest.</p>
          {runsRemaining !== null && (
            <div className="mt-4 text-xs text-white/30">
              {runsRemaining} free {runsRemaining === 1 ? 'run' : 'runs'} remaining
            </div>
          )}
        </div>
      </div>

      <div className="max-w-xl mx-auto px-6 py-8">
        <StepIndicator current={step} total={STEPS.length} />

        <div className="bg-white border border-gray-200/60 rounded-xl p-6">
          <div className="text-xs font-medium text-slate uppercase tracking-widest mb-1">Step {step + 1} of {STEPS.length}</div>
          <div className="text-lg font-medium text-ink mb-5">{STEPS[step]}</div>

          {step === 0 && <Step1Company data={company} onChange={preset ? null : setCompany} onNext={() => setStep(1)} readOnly={!!preset} />}
          {step === 1 && <Step2Objective data={objective} onChange={setObjective} onNext={() => setStep(2)} onBack={() => setStep(0)} />}
          {step === 2 && <Step3Sobjs data={sobjData} onChange={preset ? null : setSobjData} onNext={() => setStep(3)} onBack={() => setStep(1)} readOnly={!!preset} />}
          {step === 3 && <Step4Upload data={{}} onBack={() => setStep(2)} onSubmit={handleSubmit} loading={loading} error={error} byokKey={byokKey} onByokChange={setByokKey} preset={preset} presetCsvFile={presetCsvFile} presetCsvLoading={presetCsvLoading} setPresetCsvFile={setPresetCsvFile} setPresetCsvLoading={setPresetCsvLoading} />}
        </div>
      </div>
    </div>
  )
}
