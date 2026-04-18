import { useState, useEffect } from 'react'
import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || 'https://web-production-7ec13.up.railway.app'
const STORAGE_KEY = 'mk_demo_token'
const QUOTA_KEY = 'mk_demo_quota'

// Simple browser fingerprint — no library needed
function getFingerprint() {
  const nav = window.navigator
  const screen = window.screen
  const parts = [
    nav.userAgent,
    nav.language,
    screen.width + 'x' + screen.height,
    screen.colorDepth,
    new Date().getTimezoneOffset(),
    nav.hardwareConcurrency || '',
    nav.platform || '',
  ]
  // Simple hash
  let hash = 0
  const str = parts.join('|')
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash
  }
  return Math.abs(hash).toString(36)
}

export function useDemo() {
  const [token, setToken] = useState(() => localStorage.getItem(STORAGE_KEY))
  const [quota, setQuota] = useState(() => {
    try { return JSON.parse(localStorage.getItem(QUOTA_KEY)) } catch { return null }
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function requestToken(recruiterCode = null, email = null) {
    setLoading(true)
    setError(null)
    try {
      const body = { fingerprint: getFingerprint() }
      if (recruiterCode) { body.recruiter_code = recruiterCode; body.email = email }
      const r = await axios.post(`${BASE_URL}/demo/request`, body)
      const data = r.data
      localStorage.setItem(STORAGE_KEY, data.token)
      localStorage.setItem(QUOTA_KEY, JSON.stringify({
        quota_runs: data.quota_runs,
        runs_used: data.runs_used,
        access_type: data.access_type,
        expires_at: data.expires_at,
        notice: data.notice,
      }))
      setToken(data.token)
      setQuota({
        quota_runs: data.quota_runs,
        runs_used: data.runs_used,
        access_type: data.access_type,
        expires_at: data.expires_at,
        notice: data.notice,
      })
      return data
    } catch (e) {
      const msg = e.response?.data?.detail || 'Could not initialize demo session'
      setError(msg)
      throw e
    } finally {
      setLoading(false)
    }
  }

  function clearToken() {
    localStorage.removeItem(STORAGE_KEY)
    localStorage.removeItem(QUOTA_KEY)
    setToken(null)
    setQuota(null)
  }

  const runsRemaining = quota ? quota.quota_runs - quota.runs_used : null
  const isExhausted = quota ? quota.runs_used >= quota.quota_runs : false

  return { token, quota, loading, error, requestToken, clearToken, runsRemaining, isExhausted }
}
