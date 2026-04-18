import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || 'https://web-production-7ec13.up.railway.app'

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
})

export const sessions = {
  create: () => api.post('/sessions'),
  get: (id) => api.get(`/sessions/${id}`),
  setCompany: (id, data) => api.post(`/sessions/${id}/company`, data),
  setObjective: (id, data) => api.post(`/sessions/${id}/objective`, data),
  addSobj: (id, data) => api.post(`/sessions/${id}/sobjs`, data),
  approveSobj: (id, sobjId) => api.patch(`/sessions/${id}/sobjs/${sobjId}`, { status: 'approved' }),
  ingest: (id, file) => {
    const form = new FormData()
    form.append('file', file)
    return api.post(`/sessions/${id}/ingest`, form)
  },
  getJob: (id, jobId) => api.get(`/sessions/${id}/jobs/${jobId}`),
  getTaCards: (id) => api.get(`/sessions/${id}/ta-cards`),
  prefilter: (id) => api.post(`/sessions/${id}/prefilter`),
  getCandidates: (id) => api.get(`/sessions/${id}/candidates`),
  generate: (id, demoToken = null, byokKey = null) => {
    const headers = {}
    if (demoToken) headers['X-Demo-Token'] = demoToken
    if (byokKey) headers['X-Anthropic-Key'] = byokKey
    return api.post(`/sessions/${id}/generate`, {}, { headers })
  },
  getTars: (id) => api.get(`/sessions/${id}/tars`),
  getTar: (id, tarId) => api.get(`/sessions/${id}/tars/${tarId}`),
  getRankings: (id) => api.get(`/sessions/${id}/rankings`),
  getSummary: (id, tarId) => api.get(`/sessions/${id}/tars/${tarId}/summary`),
  getColumnMapping: (id) => api.get(`/sessions/${id}/column-mapping`),
  updateColumnMapping: (id, amendments) => api.patch(`/sessions/${id}/column-mapping`, { amendments }),
  // RESTORED: Working export functionality from handoff document
  export: (id) => api.get(`/sessions/${id}/export`, { responseType: 'blob' }),
}

export const examples = {
  list: () => api.get('/examples'),
  get: (slug) => api.get(`/examples/${slug}`),
  getTar: (slug, tarId) => api.get(`/examples/${slug}/tars/${tarId}`),
  getSummary: (slug, tarId) => api.get(`/examples/${slug}/tars/${tarId}/summary`),
}
