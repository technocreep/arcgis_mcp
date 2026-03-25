import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Header from '../components/Header'

export default function Projects() {
  const [projects, setProjects] = useState([])
  const [error, setError] = useState('')
  const [user, setUser] = useState(null)
  const [showUpload, setShowUpload] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [uploadPhase, setUploadPhase] = useState(null)
  const [uploadError, setUploadError] = useState(null)
  const [formId, setFormId] = useState('')
  const [gdbFile, setGdbFile] = useState(null)
  const [aprxFile, setAprxFile] = useState(null)
  const [atbxFile, setAtbxFile] = useState(null)
  const navigate = useNavigate()

  // Fetch projects and current user
  useEffect(() => {
    async function fetchProjects() {
      try {
        const res = await fetch('/api/projects/', { credentials: 'include' })
        if (!res.ok) {
          if (res.status === 401) {
            navigate('/login')
            return
          }
          const txt = await res.text().catch(() => '')
          throw new Error(txt || 'Failed to load projects')
        }
        const data = await res.json()
        setProjects(data)
      } catch (err) {
        setError(String(err))
      }
    }

    async function fetchUser() {
      try {
        const r = await fetch('/api/user_info', { credentials: 'include' })
        if (r.ok) {
          const j = await r.json()
          setUser(j?.user || null)
        }
      } catch (e) {
        // ignore
      }
    }

    fetchProjects()
    fetchUser()
  }, [navigate])

  // Helper to refresh projects list after upload/delete
  async function refreshProjects() {
    try {
      const res = await fetch('/api/projects/', { credentials: 'include' })
      if (res.ok) setProjects(await res.json())
    } catch (e) {
      // ignore
    }
  }

  // Upload implementation (based on existing static/app.js behavior)
  function startUpload() {
    setUploadError(null)
    if (!gdbFile) { setUploadError('Please select a .gdb zip archive.'); return }

    setUploading(true)
    setUploadProgress(0)
    setUploadPhase('uploading')

    const formData = new FormData()
    formData.append('project_id', formId)
    formData.append('gdb_zip', gdbFile)
    if (aprxFile) formData.append('aprx', aprxFile)
    if (atbxFile) formData.append('atbx', atbxFile)

    const xhr = new XMLHttpRequest()
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        const pct = Math.round(e.loaded / e.total * 100)
        setUploadProgress(pct)
        if (pct >= 100) setUploadPhase('processing')
      }
    }

    xhr.onload = async () => {
      setUploading(false)
      setUploadPhase(null)
      if (xhr.status === 401) {
        setUploadError('Invalid credentials. Please re-authenticate.')
      } else if (xhr.status >= 200 && xhr.status < 300) {
        setShowUpload(false)
        setFormId('')
        setGdbFile(null)
        setAprxFile(null)
        setAtbxFile(null)
        try { await refreshProjects() } catch (e) {}
      } else {
        try {
          const err = JSON.parse(xhr.responseText)
          setUploadError(err.detail || 'Upload failed')
        } catch {
          setUploadError('Upload failed')
        }
      }
    }

    xhr.onerror = () => {
      setUploading(false)
      setUploadPhase(null)
      setUploadError('Network error.')
    }

    xhr.open('POST', '/api/upload')
    xhr.withCredentials = true
    xhr.send(formData)
  }

  return (
    <div>
      <Header user={user} onChangeCredentials={() => navigate('/login')} onUpload={() => setShowUpload(true)} />

      <div className="projects-container">
        {error && <div className="auth-error">{error}</div>}

        <ul className="projects-list">
          {projects.map((p) => (
            <li key={p.id} className="project-item" onClick={() => navigate(`/projects/${encodeURIComponent(p.id)}`)} style={{ cursor: 'pointer' }}>
              <div className="card-body">
                <div className="card-header">
                  <div>
                    <div className="project-title">{p.name || p.id}</div>
                    <div className="project-meta small">ID: <span className="mono">{p.id}</span>{p.created_at ? ` • ${new Date(p.created_at).toLocaleString()}` : ''}</div>
                  </div>
                  <button className="trash-btn" onClick={(e) => { e.stopPropagation(); alert('Delete not implemented') }} title="Delete project">🗑</button>
                </div>

                <div className="card-metrics">
                  <div className="metric">
                    <div className="metric-label">Layers</div>
                    <div className="metric-value">{p.layers_count ?? '—'}</div>
                  </div>
                  <div className="metric">
                    <div className="metric-label">Attachments</div>
                    <div className="metric-value">{p.has_attachments ? '✓' : '—'}</div>
                  </div>
                </div>

                <div className="card-footer">
                  <button className="link-btn" onClick={(e) => { e.stopPropagation(); navigate(`/projects/${encodeURIComponent(p.id)}`) }}>View</button>
                  <button className="link-btn" onClick={(e) => { e.stopPropagation(); alert('Observe') }}>Observe</button>
                  <button className="link-btn" onClick={(e) => { e.stopPropagation(); alert('Data Cube') }}>Data Cube</button>
                  <button className="link-btn" onClick={(e) => { e.stopPropagation(); alert('Index KG') }}>Index KG</button>
                </div>
              </div>
            </li>
          ))}
        </ul>

        {showUpload && (
          <div className="modal-backdrop" style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="modal" style={{ background: '#fff', padding: 18, borderRadius: 8, width: 680, maxWidth: '94%' }}>
              <h3 style={{ marginTop: 0 }}>Upload Project</h3>
              <div style={{ marginBottom: 8 }}>
                <label>Project ID</label>
                <input value={formId} onChange={(e) => setFormId(e.target.value)} placeholder="project-id" style={{ width: '100%', padding: 8, marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 8 }}>
                <label>.gdb zip file</label>
                <input type="file" accept=".zip,application/zip" onChange={(e) => setGdbFile(e.target.files?.[0] || null)} style={{ display: 'block', marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 8 }}>
                <label>Optional APRX</label>
                <input type="file" accept=".aprx" onChange={(e) => setAprxFile(e.target.files?.[0] || null)} style={{ display: 'block', marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 8 }}>
                <label>Optional ATBX</label>
                <input type="file" accept=".atbx" onChange={(e) => setAtbxFile(e.target.files?.[0] || null)} style={{ display: 'block', marginTop: 4 }} />
              </div>
              {uploadError && <div style={{ color: 'crimson', marginBottom: 8 }}>{uploadError}</div>}
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <button className="link-btn" onClick={() => { setShowUpload(false); setUploadError(null) }} disabled={uploading}>Cancel</button>
                <button className="link-btn" onClick={startUpload} disabled={uploading}>Start Upload</button>
                {uploading && <div style={{ marginLeft: 8 }}>Uploading {uploadProgress}% — {uploadPhase || 'uploading'}</div>}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
