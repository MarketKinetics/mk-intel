export const downloadSession = async (sessionId, companyName = "Company") => {
  const downloadUrl = `/sessions/${sessionId}/export`;
  const link = document.createElement('a');
  link.href = downloadUrl;
  link.download = `mk_intel_${companyName.toLowerCase().replace(/\s+/g, '_')}_${sessionId.slice(0, 8)}.zip`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};
