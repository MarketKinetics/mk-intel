import { useState, useEffect, useRef } from 'react'
import { useParams, useLocation, useNavigate } from 'react-router-dom'
import { sessions } from '../api/client'
import { MappingReview } from '../components/MappingReview'

const STAGES = [
  { key: 'ingest',    label: 'Data ingestion',     desc: 'Normalizing and validating customer records' },
  { key: 'mapping',   label: 'Column mapping',      desc: 'Review and confirm field mappings' },
  { key: 'prefilter', label: 'Audience profiling',  desc: 'Mapping customers to population archetypes' },
  { key: 'generate',  label: 'Report generation',   desc: 'Generating target audience reports' },
  { key: 'done',      label: 'Analysis complete',   desc: 'Your results are ready' },
]

function StageRow({ stage, status }) {
  const isDone = status === 'done'
  const isRunning = status === 'running'
  const isPending = status === 'pending'

  return (
    <div className={`flex items-start gap-4 p-4 rounded-xl transition-all ${
      isRunning ? 'bg-teal-accent/5 border border-teal-accent/20' :
      isDone ? 'bg-surface' : 'bg-surface opacity-40'
    }`}>
      <div className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 ${
        isDone ? 'bg-teal-dark' :
        isRunning ? 'bg-teal-dark/20' :
        'bg-gray-200'
      }`}>
        {isDone ? (
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M2.5 7l3 3 6-6" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        ) : isRunning ? (
          <div className="w-3 h-3 border-2 border-teal-dark border-t-transparent rounded-full animate-spin" />
        ) : (
          <div className="w-2 h-2 rounded-full bg-gray-400" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className={`text-sm font-medium mb-0.5 ${isRunning ? 'text-teal-dark' : isDone ? 'text-ink' : 'text-slate'}`}>
          {stage.label}
        </div>
        <div className="text-xs text-slate">{stage.desc}</div>
      </div>
    </div>
  )
}

export function Processing() {
  const { sessionId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const { ingestJobId, demoToken, byokKey, company } = location.state || {}

  const [stageStatuses, setStageStatuses] = useState({
    ingest:    'running',
    mapping:   'pending',
    prefilter: 'pending',
    generate:  'pending',
    done:      'pending',
  })
  const [currentJobId, setCurrentJobId] = useState(ingestJobId)
  const [currentStage, setCurrentStage] = useState('ingest')
  const [progress, setProgress] = useState('Starting analysis...')
  const [error, setError] = useState(null)
  const [showMappingReview, setShowMappingReview] = useState(false)
  const pollerRef = useRef(null)

  useEffect(() => {
    if (!sessionId || !ingestJobId) {
      navigate('/setup')
      return
    }
    startPolling()
    return () => clearInterval(pollerRef.current)
  }, [])

  async function startPolling() {
    pollerRef.current = setInterval(async () => {
      try {
        const r = await sessions.getJob(sessionId, currentJobId)
        const job = r.data
        setProgress(job.progress || '')

        if (job.status === 'failed') {
          clearInterval(pollerRef.current)
          setError(job.error || 'Pipeline failed. Please try again.')
          return
        }

        if (job.status === 'done') {
          await advanceStage()
        }
      } catch (e) {
        console.error('Poll error:', e)
      }
    }, 3000)
  }

  async function advanceStage() {
    clearInterval(pollerRef.current)

    if (currentStage === 'ingest') {
      setStageStatuses(s => ({ ...s, ingest: 'done', mapping: 'running' }))
      setCurrentStage('mapping')
      setShowMappingReview(true)
    }
  }

  async function startPrefilter() {
    setShowMappingReview(false)
    setStageStatuses(s => ({ ...s, mapping: 'done', prefilter: 'running' }))
    setCurrentStage('prefilter')
    try {
      const r = await sessions.prefilter(sessionId)
      setCurrentJobId(r.data.job_id)
      pollerRef.current = setInterval(async () => {
        try {
          const jr = await sessions.getJob(sessionId, r.data.job_id)
          const job = jr.data
          setProgress(job.progress || '')
          if (job.status === 'failed') {
            clearInterval(pollerRef.current)
            setError(job.error || 'Profiling failed.')
          } else if (job.status === 'done') {
            await startGeneration()
          }
        } catch (e) { console.error(e) }
      }, 3000)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to start profiling.')
    }
  }
  async function startGeneration() {
    clearInterval(pollerRef.current)
    setStageStatuses(s => ({ ...s, prefilter: 'done', generate: 'running' }))
    setCurrentStage('generate')
    try {
      const r = await sessions.generate(sessionId, demoToken, byokKey)
      pollerRef.current = setInterval(async () => {
        try {
          const jr = await sessions.getJob(sessionId, r.data.job_id)
          const job = jr.data
          setProgress(job.progress || '')
          if (job.status === 'failed') {
            clearInterval(pollerRef.current)
            if (jr.data.error?.includes('quota') || jr.response?.status === 402) {
              setError('quota_exceeded')
            } else {
              setError(job.error || 'Report generation failed.')
            }
          } else if (job.status === 'done') {
            clearInterval(pollerRef.current)
            setStageStatuses(s => ({ ...s, generate: 'done', done: 'done' }))
            setCurrentStage('done')
            setProgress('Analysis complete!')
            setTimeout(() => navigate(`/session/${sessionId}`), 1500)
          }
        } catch (e) { console.error(e) }
      }, 3000)
    } catch (e) {
      if (e.response?.status === 402) {
        setError('quota_exceeded')
      } else {
        setError(e.response?.data?.detail || 'Failed to start generation.')
      }
    }
  }

  if (error === 'quota_exceeded') {
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
          <p className="text-sm text-slate mb-2 leading-relaxed">You've used all your free analysis runs.</p>
          <p className="text-sm text-slate mb-6 leading-relaxed">To run more analyses, go back to setup and enter your own Anthropic API key (~$0.15–0.20 per run).</p>
          <div className="space-y-3">
            <button onClick={() => navigate('/setup')}
              className="w-full bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90">
              Run with my own key →
            </button>
            <button onClick={() => navigate('/examples')}
              className="w-full border border-gray-200 text-slate text-sm py-3 rounded-lg hover:border-gray-300">
              Browse examples
            </button>
          </div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center px-6" style={{ background: '#F8F7F4' }}>
        <div className="bg-white border border-gray-200/60 rounded-xl p-8 max-w-md w-full text-center">
          <div className="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <circle cx="10" cy="10" r="8" stroke="#EF4444" strokeWidth="1.3"/>
              <path d="M10 6v5M10 13v.5" stroke="#EF4444" strokeWidth="1.3" strokeLinecap="round"/>
            </svg>
          </div>
          <div className="text-base font-medium text-ink mb-2">Something went wrong</div>
          <p className="text-sm text-slate mb-6 leading-relaxed">{error}</p>
          <button onClick={() => navigate('/setup')}
            className="w-full bg-teal-dark text-white text-sm font-medium py-3 rounded-lg hover:opacity-90">
            Try again
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen" style={{ background: '#F8F7F4' }}>
      <div style={{ background: 'radial-gradient(ellipse at 50% -20%, #102847 0%, #0A1628 60%)' }}
        className="px-6 py-10">
        <div className="max-w-xl mx-auto">
          <div className="text-xs font-medium text-teal-accent/60 uppercase tracking-widest mb-3">
            {company || 'Your analysis'}
          </div>
          <h1 className="text-2xl font-medium text-white/95 tracking-tight mb-2">
            {currentStage === 'done' ? 'Analysis complete' : 'Running analysis...'}
          </h1>
          <p className="text-sm text-white/45">
            {currentStage === 'done'
              ? 'Redirecting to your results...'
              : 'Analysis time varies by dataset size — feel free to leave this tab open.'}
          </p>
        </div>
      </div>

      <div className="max-w-xl mx-auto px-6 py-8">
        <div className="space-y-3 mb-6">
          {STAGES.map(stage => (
            <StageRow key={stage.key} stage={stage} status={stageStatuses[stage.key]} />
          ))}
        </div>

        {showMappingReview && (
          <div className="bg-white border border-gray-200/60 rounded-xl p-6 mb-4">
            <MappingReview sessionId={sessionId} onConfirm={startPrefilter} />
          </div>
        )}

        {progress && currentStage !== 'done' && !showMappingReview && (
          <div className="bg-white border border-gray-200/60 rounded-xl p-4 text-center">
            <div className="flex items-center justify-center gap-2 text-xs text-slate">
              <div className="w-3 h-3 border-2 border-teal-dark/30 border-t-teal-dark rounded-full animate-spin flex-shrink-0" />
              {progress}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
