import React, { useState } from 'react';
import { downloadSession } from '../utils/downloadSession';

export default function DownloadButton({ sessionId, companyName }) {
  const [downloading, setDownloading] = useState(false);
  
  const handleDownload = async () => {
    setDownloading(true);
    await downloadSession(sessionId, companyName);
    setTimeout(() => setDownloading(false), 2000);
  };

  return (
    <button
      onClick={handleDownload}
      disabled={downloading}
      style={{ backgroundColor: downloading ? '#6b7280' : '#059669' }}
      className="inline-flex items-center gap-2 px-4 py-3 rounded-lg font-semibold text-sm text-white transition-all duration-200 hover:bg-emerald-700 hover:-translate-y-0.5 disabled:opacity-60"
    >
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
        <polyline points="7,10 12,15 17,10" />
        <line x1="12" y1="15" x2="12" y2="3" />
      </svg>
      {downloading ? 'Preparing...' : 'Download Session'}
    </button>
  );
}
