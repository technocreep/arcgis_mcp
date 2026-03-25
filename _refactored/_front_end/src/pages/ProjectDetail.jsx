import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import Header from '../components/Header'
import MapView from '../components/Map/MapView'
import PointItem from '../models/PointItem'

export default function ProjectDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [project, setProject] = useState(null)
  const [manifest, setManifest] = useState(null)
  const [error, setError] = useState('')
  const [user, setUser] = useState(null)
  const [showManifest, setShowManifest] = useState(true)
  const [elements, setElements] = useState([])
  const [availableFields, setAvailableFields] = useState([])
  const [selectedFields, setSelectedFields] = useState([])
  const [fieldOpacities, setFieldOpacities] = useState({})
  const [fieldColors, setFieldColors] = useState({})

  useEffect(() => {
    if (!id) return
    async function load() {
      try {
        const res = await fetch(`/api/projects/${encodeURIComponent(id)}`, { credentials: 'include' })
        if (!res.ok) {
          if (res.status === 401) {
            navigate('/login')
            return
          }
          const txt = await res.text().catch(() => '')
          throw new Error(txt || 'Failed to load project')
        }
        const data = await res.json()
        setProject(data.summary)
        setManifest(data.manifest)
        // after project load, try to fetch datacube viz data
        try {
          const r = await fetch('/api/dataAPI/get_current_data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ project_id: data.summary?.id || id }),
          })
          if (r.ok) {
            const j = await r.json()
            if (j.status === 0 && Array.isArray(j.data)) {
              const raw = j.data
              const standardized = raw.map((item, idx) => PointItem.fromRaw(item, idx))
              setElements(standardized)
              // compute available fields
              const fs = new Set()
              standardized.forEach((p) => {
                Object.keys(p.properties || {}).forEach((k) => fs.add(k))
              })
              // remove polygon from list
              fs.delete('polygon')
              const avail = Array.from(fs)
              setAvailableFields(avail)
              setSelectedFields(avail.length > 0 ? [avail[0]] : [])
              // init opacities/colors
              const op = {}
              const cols = {}
              avail.forEach((f) => {
                op[f] = 0.9
                cols[f] = { start: '#2166ac', center: '#f4a582', end: '#b2182b' }
              })
              setFieldOpacities(op)
              setFieldColors(cols)
            }
          }
        } catch (e) {
          // ignore datacube errors
        }
      } catch (err) {
        setError(String(err))
      }
    }
    load()
    // fetch user for header display
    async function loadUser() {
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
    loadUser()
  }, [id, navigate])

  // field controls: select/unselect and per-field settings
  function selectAllFields() {
    setSelectedFields(availableFields.slice())
  }
  function unselectAllFields() {
    setSelectedFields([])
  }
  function onFieldOpacityChange(field, value) {
    setFieldOpacities((s) => ({ ...(s || {}), [field]: Number(value) }))
  }
  function onFieldColorChange(field, which, value) {
    setFieldColors((s) => ({ ...(s || {}), [field]: { ...(s?.[field] || {}), [which]: value } }))
  }

  function downloadManifest() {
    if (!manifest) return
    try {
      const blob = new Blob([JSON.stringify(manifest, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${(project && project.id) || id}-manifest.json`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      // ignore download errors
    }
  }

  function syntaxHighlight(json) {
    // Accept objects or strings
    if (json === null || json === undefined) return ''
    let text = typeof json === 'string' ? json : JSON.stringify(json, null, 2)
    // Escape HTML
    text = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')

    // Wrap tokens with span classes
    return text.replace(
      /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?)|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/g,
      function (match) {
        let cls = 'json-number'
        if (/^"/.test(match)) {
          if (/:\s*$/.test(match)) {
            cls = 'json-key'
          } else {
            cls = 'json-string'
          }
        } else if (/true|false/.test(match)) {
          cls = 'json-boolean'
        } else if (/null/.test(match)) {
          cls = 'json-null'
        }
        return `<span class="${cls}">${match}</span>`
      }
    )
  }

  // Render JSON as nested <details> for collapsible nodes.
  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;')
  }

  function renderPrimitive(val) {
    if (val === null) return '<span class="json-null">null</span>'
    if (typeof val === 'boolean') return `<span class="json-boolean">${val}</span>`
    if (typeof val === 'number') return `<span class="json-number">${val}</span>`
    // string
    return `<span class="json-string">"${escapeHtml(val)}"</span>`
  }

  function renderRow(key, value) {
    const keyHtml = `<span class="json-key">"${escapeHtml(key)}"</span>`
    if (value && typeof value === 'object') {
      return `${keyHtml}: ${toDetails(value)}`
    }
    return `${keyHtml}: ${renderPrimitive(value)}`
  }

  function toDetails(value) {
    if (value === null) return renderPrimitive(null)
    if (Array.isArray(value)) {
      if (value.length === 0) return '<span>[]</span>'
      const children = value.map((v, i) => `<div class="json-child">${renderRow(i, v)}</div>`).join('')
      return `<details open class="json-details"><summary>Array[${value.length}]</summary>${children}</details>`
    }
    if (typeof value === 'object') {
      const keys = Object.keys(value)
      if (keys.length === 0) return '<span>{}</span>'
      const children = keys.map((k) => `<div class="json-child">${renderRow(k, value[k])}</div>`).join('')
      return `<details open class="json-details"><summary>Object{${keys.length}}</summary>${children}</details>`
    }
    return renderPrimitive(value)
  }

  function renderJsonInteractive(json) {
    if (json === null || json === undefined) return ''
    // If it's not an object/array, just show primitive
    if (typeof json !== 'object') return renderPrimitive(json)
    return toDetails(json)
  }

  // Render JSON as a code-like view with line numbers and collapsible folds
  function renderCodeWithFolds(obj) {
    let text = JSON.stringify(obj, null, 2)
    const lines = text.split('\n')

    // Find fold ranges by tracking braces
    const stack = []
    const ranges = []
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i]
      // count opening braces/brackets
      if (/[\{\[]\s*$/.test(line)) {
        stack.push({ start: i })
      }
      if (/^[ \t]*[\}\]]/.test(line)) {
        const last = stack.pop()
        if (last) {
          last.end = i
          ranges.push(last)
        }
      }
    }

    // map starts and ends to ids
    const startMap = {}
    const endMap = {}
    ranges.forEach((r, idx) => {
      const id = `r${idx}`
      startMap[r.start] = id
      endMap[r.end] = id
    })

    function highlightLine(line) {
      // reuse the same token regex from syntaxHighlight
      return line.replace(/(&|<|>)/g, (m) => (m === '&' ? '&amp;' : m === '<' ? '&lt;' : '&gt;'))
        .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)? )|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/g,
          function (match) {
            let cls = 'json-number'
            if (/^"/.test(match)) {
              if (/:\s*$/.test(match)) {
                cls = 'json-key'
              } else {
                cls = 'json-string'
              }
            } else if (/true|false/.test(match)) {
              cls = 'json-boolean'
            } else if (/null/.test(match)) {
              cls = 'json-null'
            }
            return `<span class="${cls}">${match}</span>`
          }
        )
    }

    let out = ''
    const openRanges = []
    for (let i = 0; i < lines.length; i++) {
      // close ranges ending at this line before writing it
      if (endMap[i]) {
        // close corresponding wrapper
        out += `</div>`
        const id = endMap[i]
        const idx = openRanges.lastIndexOf(id)
        if (idx !== -1) openRanges.splice(idx, 1)
      }

      const ln = i + 1
      const hasStart = !!startMap[i]
      const foldId = startMap[i]
      const lineHtml = highlightLine(lines[i])

      if (hasStart) {
        // start line with toggle and then open a fold-group for inner lines
        out += `<div class="code-line"><span class="gutter">${ln}</span><span class="fold-toggle" data-range-id="${foldId}">▾</span><span class="collapsed-placeholder" style="display:none;">&gt;</span><span class="code-content">${lineHtml}</span></div>`
        out += `<div class="fold-group" data-range-id="${foldId}">`
        openRanges.push(foldId)
      } else {
        out += `<div class="code-line"><span class="gutter">${ln}</span><span class="code-content">${lineHtml}</span></div>`
      }
    }
    // close any remaining open groups
    while (openRanges.length) {
      out += `</div>`
      openRanges.pop()
    }

    // Wrap with container that includes a code block
    return `<div class="code-view"><div class="code-block">${out}</div></div>`
  }

  // Attach folding handlers after render
  useEffect(() => {
    if (!showManifest) return
    const container = document.querySelector('.manifest-viewer')
    if (!container) return
    const toggles = container.querySelectorAll('.fold-toggle')
    toggles.forEach((t) => {
      // avoid adding duplicate listeners
      if (t._hasHandler) return
      t._hasHandler = true
      t.addEventListener('click', (e) => {
        const id = t.getAttribute('data-range-id')
        const group = container.querySelector(`.fold-group[data-range-id='${id}']`)
        if (!group) return
        group.classList.toggle('collapsed')
        // update arrow
        t.textContent = group.classList.contains('collapsed') ? '▸' : '▾'
        // toggle collapsed class on the corresponding code line so we can show '>' placeholder
        const line = t.closest('.code-line')
        if (line) line.classList.toggle('collapsed')
      })
    })
  }, [manifest, showManifest])

  return (
    <div>
      <Header user={user} onChangeCredentials={() => navigate('/login')} onUpload={() => alert('Upload not implemented')} />
      <div className="projects-container" style={{ minHeight: '90vh' }}>
        <button onClick={() => navigate(-1)} style={{ marginBottom: 12 }}>← Back</button>
      {error && <div className="auth-error">{error}</div>}

      {project ? (
        <div className="project-detail">
          <h1 style={{ margin: 20, color: 'black' }}>{project.name || project.id}</h1>
          <div className="project-meta">ID: <span className="mono">{project.id}</span></div>
          {project.created_at && <div className="project-meta">Created: {new Date(project.created_at).toLocaleString()}</div>}
          <div className="project-meta">Layers: {project.layers_count}</div>
          <div className="project-meta">Attachments: {project.has_attachments ? 'yes' : 'no'}</div>
          {project.gdb_file && <div className="project-meta">GDB: {project.gdb_file}</div>}

          <div style={{ marginTop: 18 }}>
            <div style={{ display: 'flex', alignItems: 'center', color: 'black', gap: 8, marginBottom: 8 }}>
              <h2 style={{ margin: 0, color: 'black' }}>Manifest</h2>
              <button className="link-btn" onClick={() => setShowManifest((s) => !s)}>{showManifest ? 'Hide' : 'Show'}</button>
              <button className="link-btn" onClick={downloadManifest} disabled={!manifest}>Download</button>
            </div>

            {showManifest && (
              <div
                className="manifest-viewer"
                dangerouslySetInnerHTML={{ __html: manifest ? renderJsonInteractive(manifest) : 'No manifest available' }}
              />
            )}
          </div>

          {/* Map preview (if datacube elements available) */}
          {elements && elements.length > 0 && (
            <div style={{ marginTop: 18 }}>
              <h2 style={{ margin: '8px 0' }}>Map Preview</h2>
              <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
                <div style={{ width: 260 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <h4 style={{ marginTop: 0 }}>Fields</h4>
                    <div>
                      <button className="link-btn" onClick={selectAllFields} style={{ marginRight: 6 }}>Select all</button>
                      <button className="link-btn" onClick={unselectAllFields}>Unselect</button>
                    </div>
                  </div>

                  {availableFields.length === 0 && <div>No fields</div>}
                  <div style={{ maxHeight: '60vh', overflowY: 'auto', paddingRight: 6 }}>
                    {availableFields.map((f) => (
                      <div key={f} style={{ marginBottom: 12, borderBottom: '1px solid #eee', paddingBottom: 8 }}>
                        <label style={{ display: 'block', marginBottom: 6 }}>
                          <input type="checkbox" checked={selectedFields.includes(f)} onChange={() => {
                            const next = selectedFields.includes(f) ? selectedFields.filter(s => s !== f) : [...selectedFields, f]
                            setSelectedFields(next)
                          }} />&nbsp;{f}
                        </label>

                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <input type="range" min="0" max="1" step="0.01" value={fieldOpacities[f] ?? 0.9} onChange={(e) => onFieldOpacityChange(f, e.target.value)} style={{ flex: 1 }} />
                          <div style={{ width: 44, textAlign: 'right', fontSize: 12 }}>{Math.round((fieldOpacities[f] ?? 0.9) * 100)}%</div>
                        </div>

                        <div style={{ marginTop: 8, display: 'flex', gap: 6, alignItems: 'center' }}>
                          <input type="color" value={fieldColors[f]?.start ?? '#2166ac'} onChange={(e) => onFieldColorChange(f, 'start', e.target.value)} />
                          <input type="color" value={fieldColors[f]?.center ?? '#f4a582'} onChange={(e) => onFieldColorChange(f, 'center', e.target.value)} />
                          <input type="color" value={fieldColors[f]?.end ?? '#b2182b'} onChange={(e) => onFieldColorChange(f, 'end', e.target.value)} />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                <div style={{ flex: 1, height: '70vh', border: '1px solid #ccc' }}>
                  <MapView elements={elements} selectedFields={selectedFields} fieldOpacities={fieldOpacities} fieldColors={fieldColors} />
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <div>Loading…</div>
      )}
      </div>
    </div>
  )
}
