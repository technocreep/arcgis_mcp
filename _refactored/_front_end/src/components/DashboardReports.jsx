import { useEffect, useMemo, useState } from 'react'
import './DashboardReports.css'

const DASHBOARD_PAGES = [
  { file: 'dashboard/viz.html', icon: '🗺', title: 'Visualization Hub', desc: 'Галерея карт, профили, сравнения' },
  { file: 'dashboard/index.html', icon: '📊', title: 'Data Mart', desc: 'Метрики и ключевые ссылки' },
  { file: 'dashboard/data.html', icon: '📋', title: 'Data Preview', desc: 'Артефакты CSV, статистики' },
  { file: 'dashboard/metrics.html', icon: '📈', title: 'Eval Metrics', desc: 'Capture curves, AUC' },
  { file: 'dashboard/artifacts.html', icon: '🗂', title: 'Artifacts Registry', desc: 'Все файлы пайплайна' },
]

function collectConfigPills(manifest) {
  const pills = []
  const project = manifest?.project || {}
  const pipeline = manifest?.pipeline || manifest?.experiment?.pipeline || {}
  const cubeSpec = pipeline?.cube_spec || manifest?.cube_spec || {}
  const rsSpec = pipeline?.rs_spec || manifest?.rs_spec || {}
  const recipes = manifest?.viz?.recipes || manifest?.recipes

  if (cubeSpec?.step_m) pills.push(`step=${cubeSpec.step_m}m`)
  if (cubeSpec?.fault_radius_m) pills.push(`fault_r=${cubeSpec.fault_radius_m}m`)
  if (rsSpec?.enabled) pills.push('RS=on')
  if (manifest?.run_interpretability === false) pills.push('interp=off')
  if (Array.isArray(recipes) && recipes.length) pills.push(`recipes=${recipes.length}`)
  if (project?.primary_crs) pills.push(project.primary_crs)
  return pills
}

export default function DashboardReports({ projectId, manifest }) {
  const [files, setFiles] = useState([])
  const [exists, setExists] = useState(null)
  const [error, setError] = useState('')
  const [selectedPage, setSelectedPage] = useState('')
  const pills = useMemo(() => collectConfigPills(manifest), [manifest])

  useEffect(() => {
    if (!projectId) return
    let cancelled = false
    async function loadFiles() {
      setError('')
      try {
        const response = await fetch(`/api/projects/${encodeURIComponent(projectId)}/datacube/files`, {
          credentials: 'include',
        })
        if (!response.ok) {
          const text = await response.text().catch(() => '')
          throw new Error(text || 'Failed to load dashboard files')
        }
        const data = await response.json()
        if (cancelled) return
        const nextFiles = Array.isArray(data.files) ? data.files : []
        setExists(Boolean(data.exists))
        setFiles(nextFiles)
        const availablePages = DASHBOARD_PAGES.filter((page) => nextFiles.includes(page.file))
        setSelectedPage((current) => current && nextFiles.includes(current) ? current : (availablePages[0]?.file || ''))
      } catch (err) {
        if (!cancelled) {
          setError(String(err))
          setExists(false)
          setFiles([])
          setSelectedPage('')
        }
      }
    }
    loadFiles()
    return () => {
      cancelled = true
    }
  }, [projectId])

  const availablePages = DASHBOARD_PAGES.filter((page) => files.includes(page.file))
  const iframeSrc = selectedPage
    ? `/api/projects/${encodeURIComponent(projectId)}/datacube/files/${selectedPage}`
    : ''

  return (
    <section className="dashboard-reports card">
      <div className="dashboard-header-row">
        <div>
          <h2>Dashboard & Reports</h2>
          <p className="dashboard-subtitle">Отчёты пайплайна и интерактивные артефакты для проекта.</p>
        </div>
        {selectedPage && (
          <a
            className="dashboard-open-link"
            href={iframeSrc}
            target="_blank"
            rel="noopener noreferrer"
          >
            Open in new tab
          </a>
        )}
      </div>

      {pills.length > 0 && (
        <div className="config-strip dashboard-config-strip">
          {pills.map((pill) => (
            <span key={pill} className="config-pill">{pill}</span>
          ))}
        </div>
      )}

      {error && <div className="dashboard-error">{error}</div>}

      {!error && exists === false && (
        <div className="dashboard-empty">
          Dashboard pages not yet generated.<br />
          Run the pipeline to produce <code>dashboard/</code> artifacts.
        </div>
      )}

      {!error && exists && availablePages.length === 0 && (
        <div className="dashboard-empty">
          Data Cube exists, but no dashboard HTML pages were found.
        </div>
      )}

      {availablePages.length > 0 && (
        <>
          <div className="dash-grid">
            {availablePages.map((page) => {
              const isActive = selectedPage === page.file
              return (
                <button
                  key={page.file}
                  type="button"
                  className={`dash-card dash-card-button${isActive ? ' active' : ''}`}
                  onClick={() => setSelectedPage(page.file)}
                >
                  <div className="dash-card-icon">{page.icon}</div>
                  <div className="dash-card-title">{page.title}</div>
                  <div className="dash-card-desc">{page.desc}</div>
                </button>
              )
            })}
          </div>

          {iframeSrc && (
            <div className="dashboard-frame-wrap">
              <iframe
                key={iframeSrc}
                title="Dashboard report preview"
                className="dashboard-frame"
                src={iframeSrc}
              />
            </div>
          )}
        </>
      )}
    </section>
  )
}
