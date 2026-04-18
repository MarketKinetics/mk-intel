import React, { useState } from 'react'
import { sessions as sessionsApi } from '../api/client'

const DownloadButton = ({ sessionId, companyName }) => {
  const [downloading, setDownloading] = useState(false)

  const handleDownload = async () => {
    try {
      setDownloading(true)
      
      // Use the documented export API from handoff document
      const response = await sessionsApi.export(sessionId)
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]))
      const link = document.createElement('a')
      link.href = url
      
      // Generate filename: CompanyName_SessionID.zip
      const filename = `${(companyName || 'Analysis').replace(/[^a-zA-Z0-9]/g, '_')}_${sessionId.slice(0, 8)}.zip`
      link.setAttribute('download', filename)
      
      // Trigger download
      document.body.appendChild(link)
      link.click()
      link.remove()
      
      // Cleanup
      window.URL.revokeObjectURL(url)
    } catch (error) {
      console.error('Download failed:', error)
      // Could add toast notification here
    } finally {
      setDownloading(false)
    }
  }

  return (
    <button
      onClick={handleDownload}
      disabled={downloading}
      className="flex items-center gap-2 px-3 py-2 text-xs font-medium text-white/90 hover:text-white border border-white/20 hover:border-white/30 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {downloading ? (
        <>
          <div className="w-3 h-3 border border-white/40 border-t-white/90 rounded-full animate-spin" />
          Preparing...
        </>
      ) : (
        <>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M7 1v8m0 0L4 6m3 3l3-3M1 12h12" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Download
        </>
      )}
    </button>
  )
}

export default DownloadButton
