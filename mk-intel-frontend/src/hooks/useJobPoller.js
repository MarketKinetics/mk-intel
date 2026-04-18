import { useState, useEffect, useRef } from 'react'
import { sessions } from '../api/client'

export function useJobPoller(sessionId, jobId, onComplete) {
  const [job, setJob] = useState(null)
  const [error, setError] = useState(null)
  const intervalRef = useRef(null)

  useEffect(() => {
    if (!sessionId || !jobId) return

    const poll = async () => {
      try {
        const res = await sessions.getJob(sessionId, jobId)
        setJob(res.data)
        if (res.data.status === 'done') {
          clearInterval(intervalRef.current)
          onComplete && onComplete(res.data)
        }
        if (res.data.status === 'failed') {
          clearInterval(intervalRef.current)
          setError(res.data.error)
        }
      } catch (err) {
        clearInterval(intervalRef.current)
        setError(err.message)
      }
    }

    poll()
    intervalRef.current = setInterval(poll, 3000)
    return () => clearInterval(intervalRef.current)
  }, [sessionId, jobId])

  return { job, error }
}
