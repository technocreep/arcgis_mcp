import './DataCubeViewer.css'

export default function DataCubeViewer({ projectId }) {
  if (!projectId) return null

  const src = `/ui/datacube/index.html?project_id=${encodeURIComponent(projectId)}`

  return (
    <section className="data-cube-viewer card">
      <div className="data-cube-viewer__header">
        <div>
          <h2>Data Cube Viewer</h2>
          <p className="data-cube-viewer__subtitle">Элементы V0–V10, перенесённые в `_refactored` без зависимостей от `arcgis_mcp`.</p>
        </div>
        <a className="data-cube-viewer__link" href={src} target="_blank" rel="noopener noreferrer">
          Open full viewer
        </a>
      </div>
      <iframe
        key={src}
        title="Data Cube Viewer"
        className="data-cube-viewer__frame"
        src={src}
      />
    </section>
  )
}
