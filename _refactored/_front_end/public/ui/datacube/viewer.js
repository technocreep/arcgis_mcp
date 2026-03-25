'use strict'

const state = {
  projectId: null,
  files: new Set(),
  blocks: [],
  evalReport: {},
  modelMeta: {},
  runMeta: {},
  experimentConfig: {},
  rsStatus: {},
  globalImportance: [],
  ale: [],
  shapValues: [],
  dominantDriver: [],
  shapGeoUnit: [],
  scoreMap: new Map(),
  driverMap: new Map(),
  v1Rects: [],
  v1SelectedId: null,
  v1View: { scale: 1, panX: 0, panY: 0 },
}

const charts = {}

function splitCSVLine(line) {
  const result = []
  let current = ''
  let inQuotes = false
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i]
    if (char === '"') inQuotes = !inQuotes
    else if (char === ',' && !inQuotes) {
      result.push(current)
      current = ''
    } else current += char
  }
  result.push(current)
  return result
}

function parseCSV(text) {
  if (!text) return []
  const lines = text.trim().split(/\r?\n/)
  if (lines.length < 2) return []
  const headers = splitCSVLine(lines[0]).map((header) => header.replace(/^"|"$/g, '').trim())
  return lines.slice(1).filter((line) => line.trim()).map((line) => {
    const values = splitCSVLine(line)
    const row = {}
    headers.forEach((header, index) => {
      const raw = (values[index] || '').replace(/^"|"$/g, '').trim()
      row[header] = (raw === '' || raw === 'nan' || raw === 'NaN' || raw === 'None') ? null : (Number.isNaN(Number(raw)) ? raw : parseFloat(raw))
    })
    return row
  })
}

function fileUrl(path) {
  return `/api/projects/${encodeURIComponent(state.projectId)}/datacube/files/${path}`
}

function authOpts() {
  return { credentials: 'include' }
}

async function fetchText(path) {
  try {
    const response = await fetch(fileUrl(path), authOpts())
    return response.ok ? response.text() : null
  } catch {
    return null
  }
}

async function fetchJSON(path) {
  try {
    const response = await fetch(fileUrl(path), authOpts())
    return response.ok ? response.json() : {}
  } catch {
    return {}
  }
}

function viridis(t) {
  const stops = [[68, 1, 84], [62, 74, 137], [33, 145, 140], [94, 201, 98], [253, 231, 37]]
  const scaled = Math.min(Math.max(t, 0), 1) * (stops.length - 1)
  const low = Math.floor(scaled)
  const high = Math.min(low + 1, stops.length - 1)
  const fraction = scaled - low
  const color = stops[low].map((value, index) => Math.round(value + fraction * (stops[high][index] - value)))
  return `rgb(${color[0]},${color[1]},${color[2]})`
}

function diverging(t) {
  const clamped = Math.min(Math.max(t, -1), 1)
  if (clamped < 0) {
    const scale = -clamped
    return `rgb(${Math.round(255 * (1 - scale))},${Math.round(255 * (1 - scale))},255)`
  }
  return `rgb(255,${Math.round(255 * (1 - clamped))},${Math.round(255 * (1 - clamped))})`
}

const CAT_PALETTE = ['#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f', '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac']
function catColor(index) {
  return CAT_PALETTE[index % CAT_PALETTE.length]
}

function setBody(id, html) {
  const element = document.getElementById(`${id}-body`) || document.getElementById(id)
  if (element) element.innerHTML = html
}

function showNA(bodyId, message) {
  setBody(bodyId, `<div class="na-msg">${message || 'Data not available'}</div>`)
}

function destroyChart(key) {
  if (charts[key]) {
    charts[key].destroy()
    delete charts[key]
  }
}

async function loadAll() {
  try {
    const response = await fetch(`/api/projects/${encodeURIComponent(state.projectId)}/datacube`, authOpts())
    if (response.ok) {
      const data = await response.json()
      state.files = new Set(data.files || [])
    }
  } catch {}

  const [
    blocksTxt,
    scoresTxt,
    evalReport,
    modelMeta,
    runMeta,
    importanceTxt,
    aleTxt,
    shapTxt,
    driverTxt,
    geoShapTxt,
    experimentConfig,
    rsStatus,
  ] = await Promise.all([
    fetchText('blocks.csv'),
    fetchText('scores.csv'),
    fetchJSON('eval_report.json'),
    fetchJSON('model_meta.json'),
    fetchJSON('run_meta.json'),
    fetchText('interpretability/global_importance_features.csv'),
    fetchText('interpretability/ale_1d.csv'),
    fetchText('interpretability/shap_values.csv'),
    fetchText('interpretability/dominant_driver_group.csv'),
    fetchText('interpretability/shap_geo_unit_summary.csv'),
    fetchJSON('experiment_config.json'),
    fetchJSON('rs/rs_stage_status.json'),
  ])

  state.blocks = parseCSV(blocksTxt)
  state.evalReport = evalReport || {}
  state.modelMeta = modelMeta || {}
  state.runMeta = runMeta || {}
  state.experimentConfig = experimentConfig || {}
  state.rsStatus = rsStatus || {}
  state.globalImportance = parseCSV(importanceTxt)
  state.ale = parseCSV(aleTxt)
  state.shapValues = parseCSV(shapTxt)
  state.dominantDriver = parseCSV(driverTxt)
  state.shapGeoUnit = parseCSV(geoShapTxt)

  const scores = parseCSV(scoresTxt)
  scores.forEach((scoreRow) => state.scoreMap.set(String(scoreRow.block_id ?? scoreRow[Object.keys(scoreRow)[0]]), scoreRow.score ?? scoreRow[Object.keys(scoreRow)[1]]))

  state.dominantDriver.forEach((row) => {
    const keys = Object.keys(row)
    const idCol = keys.find((key) => key === 'block_id') || keys[0]
    const groupCol = keys.find((key) => key === 'dominant_driver_group') || keys.find((key) => key !== idCol) || keys[1]
    state.driverMap.set(String(row[idCol]), row[groupCol])
  })
}

function drawChoropleth(canvasId, tooltipId, colorFn, onClickBlock, view) {
  const canvas = document.getElementById(canvasId)
  if (!canvas || !state.blocks.length) return
  const ctx = canvas.getContext('2d')
  const width = canvas.width
  const height = canvas.height
  ctx.clearRect(0, 0, width, height)
  ctx.fillStyle = '#f8fafc'
  ctx.fillRect(0, 0, width, height)

  const sample = state.blocks[0]
  const keys = Object.keys(sample)
  const hasMetric = keys.includes('x_m') && keys.includes('y_m')
  const hasCellSize = keys.includes('cell_size_m')
  let xs
  let ys
  let xe
  let ye

  if (hasMetric && hasCellSize) {
    const half = sample.cell_size_m / 2
    xs = state.blocks.map((block) => block.x_m - half)
    ys = state.blocks.map((block) => block.y_m - half)
    xe = state.blocks.map((block) => block.x_m + half)
    ye = state.blocks.map((block) => block.y_m + half)
  } else {
    const xCol = keys.find((key) => ['x_m', 'lon', 'longitude', 'x', 'cx', 'x_center'].includes(key))
    const yCol = keys.find((key) => ['y_m', 'lat', 'latitude', 'y', 'cy', 'y_center'].includes(key))
    if (!xCol || !yCol) {
      ctx.fillStyle = '#64748b'
      ctx.font = '14px sans-serif'
      ctx.textAlign = 'center'
      ctx.fillText('Cannot detect geometry columns', width / 2, height / 2)
      return
    }
    const centers = [...new Set(state.blocks.map((block) => block[xCol]))].sort((a, b) => a - b)
    const diffs = centers.slice(1).map((value, index) => value - centers[index]).filter((diff) => diff > 0).sort((a, b) => a - b)
    const step = diffs[Math.floor(diffs.length / 2)] || 5000
    xs = state.blocks.map((block) => block[xCol] - step / 2)
    ys = state.blocks.map((block) => block[yCol] - step / 2)
    xe = state.blocks.map((block) => block[xCol] + step / 2)
    ye = state.blocks.map((block) => block[yCol] + step / 2)
  }

  const valid = (values) => values.filter((value) => value != null && Number.isFinite(value))
  const minX = Math.min(...valid(xs))
  const maxX = Math.max(...valid(xe))
  const minY = Math.min(...valid(ys))
  const maxY = Math.max(...valid(ye))
  const rangeX = (maxX - minX) || 1
  const rangeY = (maxY - minY) || 1
  const padding = 12
  const mapAspect = rangeX / rangeY
  const canvasAspect = width / height
  let drawW = width - 2 * padding
  let drawH = height - 2 * padding
  let offX = padding
  let offY = padding
  if (mapAspect > canvasAspect) {
    drawH = drawW / mapAspect
    offY = (height - drawH) / 2
  } else {
    drawW = drawH * mapAspect
    offX = (width - drawW) / 2
  }

  const baseToX = (x) => offX + ((x - minX) / rangeX) * drawW
  const baseToY = (y) => offY + ((maxY - y) / rangeY) * drawH
  const scale = view?.scale || 1
  const panX = view?.panX || 0
  const panY = view?.panY || 0
  const centerX = width / 2
  const centerY = height / 2
  const toX = (x) => (baseToX(x) - centerX) * scale + centerX + panX
  const toY = (y) => (baseToY(y) - centerY) * scale + centerY + panY

  const idCol = keys.find((key) => key === 'block_id') || keys[0]
  const rects = []

  state.blocks.forEach((block, index) => {
    if (xs[index] == null) return
    const x1 = toX(xs[index])
    const y1 = toY(ye[index])
    const blockW = Math.max(toX(xe[index]) - x1, 2)
    const blockH = Math.max(toY(ys[index]) - y1, 2)
    const blockId = String(block[idCol])
    ctx.fillStyle = colorFn(blockId) || '#e2e8f0'
    ctx.fillRect(x1, y1, blockW, blockH)
    ctx.strokeStyle = 'rgba(255,255,255,0.4)'
    ctx.lineWidth = 0.5
    ctx.strokeRect(x1, y1, blockW, blockH)
    rects.push({ id: blockId, x: x1, y: y1, w: blockW, h: blockH })
  })

  if (canvasId === 'v1-canvas') state.v1Rects = rects

  function highlightBlock(blockId, color, lineWidth) {
    const rect = rects.find((item) => item.id === blockId)
    if (!rect) return
    ctx.strokeStyle = color
    ctx.lineWidth = lineWidth
    ctx.strokeRect(rect.x + 0.5, rect.y + 0.5, rect.w - 1, rect.h - 1)
  }

  if (state.v1SelectedId && canvasId === 'v1-canvas') highlightBlock(state.v1SelectedId, '#0ea5e9', 3)

  if (onClickBlock) {
    canvas.style.cursor = 'crosshair'
    canvas.onclick = (event) => {
      const rect = canvas.getBoundingClientRect()
      const scl = canvas.width / rect.width
      const mx = (event.clientX - rect.left) * scl
      const my = (event.clientY - rect.top) * scl
      for (const blockRect of rects) {
        if (mx >= blockRect.x && mx <= blockRect.x + blockRect.w && my >= blockRect.y && my <= blockRect.y + blockRect.h) {
          state.v1SelectedId = blockRect.id
          drawChoropleth(canvasId, tooltipId, colorFn, onClickBlock, view)
          onClickBlock(blockRect.id)
          break
        }
      }
    }

    const tooltip = tooltipId ? document.getElementById(tooltipId) : null
    canvas.onmousemove = (event) => {
      if (view?._dragging) return
      const rect = canvas.getBoundingClientRect()
      const scl = canvas.width / rect.width
      const mx = (event.clientX - rect.left) * scl
      const my = (event.clientY - rect.top) * scl
      const found = rects.find((blockRect) => mx >= blockRect.x && mx <= blockRect.x + blockRect.w && my >= blockRect.y && my <= blockRect.y + blockRect.h)
      if (!tooltip) return
      if (found) {
        const score = state.scoreMap.get(found.id)
        tooltip.innerHTML = `<b>${found.id}</b><br>Score: ${score != null ? Number(score).toFixed(3) : '—'}`
        const containerRect = canvas.parentElement.getBoundingClientRect()
        tooltip.style.left = `${event.clientX - containerRect.left + 12}px`
        tooltip.style.top = `${event.clientY - containerRect.top + 12}px`
        tooltip.style.display = 'block'
        canvas.style.cursor = 'pointer'
      } else {
        tooltip.style.display = 'none'
        canvas.style.cursor = scale > 1 ? 'grab' : 'crosshair'
      }
    }
    canvas.onmouseleave = () => {
      if (tooltip) tooltip.style.display = 'none'
    }
  }
}

async function renderV0() {
  if (!state.files.has('viz/map.html')) {
    showNA('v0', 'Interactive map (viz/map.html) not found')
    return
  }
  try {
    const response = await fetch(fileUrl('viz/map.html'), authOpts())
    if (!response.ok) {
      showNA('v0', 'Failed to load map')
      return
    }
    const html = await response.text()
    const blob = new Blob([html], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    setBody('v0', `<iframe src="${blobUrl}" class="map-iframe" title="Prospectivity Map"></iframe>`)
  } catch {
    showNA('v0', 'Failed to load map')
  }
}

function renderV3() {
  const report = state.evalReport
  const meta = state.modelMeta
  const captureEfficiency = report.capture_efficiency || {}
  const format = (value) => value != null ? Number(value).toFixed(3) : 'N/A'
  const metrics = [
    { label: 'PR-AUC (test)', value: format(report.metrics?.pr_auc), icon: '◉', accent: 'emerald' },
    { label: 'CV PR-AUC', value: format(meta.cv?.mean_pr_auc), icon: '◎', accent: 'blue' },
    { label: 'x* (volume)', value: format(captureEfficiency.x_star), icon: '◈', accent: 'violet' },
    { label: 'Score @ x*', value: format(captureEfficiency.score_threshold_at_x_star), icon: '◇', accent: 'amber' },
  ]
  setBody('v3', `
    <div class="metric-grid">
      ${metrics.map(({ label, value, icon, accent }) => `
        <div class="metric-card accent-${accent}">
          <div class="metric-icon">${icon}</div>
          <div class="metric-value">${value}</div>
          <div class="metric-label">${label}</div>
        </div>`).join('')}
    </div>
    <div style="margin-top:10px;font-size:11px;color:#94a3b8;text-align:right">
      Model: <b style="color:#475569">${meta.model_type || '—'}</b> &nbsp;|&nbsp;
      CV splits: <b style="color:#475569">${meta.cv?.effective_splits ?? meta.cv?.requested_splits ?? '—'}</b>
    </div>
  `)
}

function renderMetaValue(value, depth) {
  if (value == null) return '<span style="color:#94a3b8">—</span>'
  if (Array.isArray(value)) {
    if (!value.length) return '<span style="color:#94a3b8">[]</span>'
    if (value.every((item) => typeof item !== 'object')) return value.join(', ')
    return `<code style="font-size:11px">${JSON.stringify(value)}</code>`
  }
  if (typeof value === 'object') {
    if (depth >= 1) return `<code style="font-size:11px">${JSON.stringify(value)}</code>`
    const inner = Object.entries(value).map(([key, nested]) => `<span class="meta-sub-key">${key}</span><span>${renderMetaValue(nested, depth + 1)}</span>`).join('')
    return `<div class="meta-sub">${inner}</div>`
  }
  return String(value)
}

function renderV9() {
  const meta = state.modelMeta
  if (!Object.keys(meta).length) {
    showNA('v9')
    return
  }
  const rows = Object.entries(meta).map(([key, value]) => `<tr><td class="meta-key">${key}</td><td class="meta-val">${renderMetaValue(value, 0)}</td></tr>`).join('')
  setBody('v9', `<table class="meta-table"><tbody>${rows}</tbody></table>`)
}

function renderV1() {
  if (!state.blocks.length) {
    showNA('v1', 'No blocks.csv data')
    return
  }
  const scoreValues = [...state.scoreMap.values()].filter((value) => value != null).map(Number)
  if (!scoreValues.length) {
    showNA('v1', 'No scores data')
    return
  }
  const minS = Math.min(...scoreValues)
  const maxS = Math.max(...scoreValues)
  const range = (maxS - minS) || 1
  const colorFn = (blockId) => {
    const score = state.scoreMap.get(blockId)
    return score != null ? viridis((Number(score) - minS) / range) : '#e2e8f0'
  }
  const clickFn = (blockId) => renderV7(blockId)
  drawChoropleth('v1-canvas', 'v1-tooltip', colorFn, clickFn, state.v1View)

  const canvas = document.getElementById('v1-canvas')
  if (canvas && !canvas._zoomBound) {
    canvas._zoomBound = true
    const redraw = () => drawChoropleth('v1-canvas', 'v1-tooltip', colorFn, clickFn, state.v1View)
    canvas.addEventListener('wheel', (event) => {
      event.preventDefault()
      const zoomFactor = event.deltaY < 0 ? 1.25 : 1 / 1.25
      const rect = canvas.getBoundingClientRect()
      const scl = canvas.width / rect.width
      const mx = (event.clientX - rect.left) * scl
      const my = (event.clientY - rect.top) * scl
      const cx = canvas.width / 2
      const cy = canvas.height / 2
      const newScale = Math.max(1, Math.min(16, state.v1View.scale * zoomFactor))
      const ratio = newScale / state.v1View.scale
      state.v1View.panX = (mx - cx) * (1 - ratio) + state.v1View.panX * ratio
      state.v1View.panY = (my - cy) * (1 - ratio) + state.v1View.panY * ratio
      state.v1View.scale = newScale
      if (newScale === 1) {
        state.v1View.panX = 0
        state.v1View.panY = 0
      }
      redraw()
    }, { passive: false })

    let dragStart = null
    canvas.addEventListener('mousedown', (event) => {
      if (state.v1View.scale > 1) {
        dragStart = { x: event.clientX, y: event.clientY, px: state.v1View.panX, py: state.v1View.panY }
        state.v1View._dragging = false
        canvas.style.cursor = 'grabbing'
      }
    })
    window.addEventListener('mousemove', (event) => {
      if (!dragStart) return
      const rect = canvas.getBoundingClientRect()
      const scl = canvas.width / rect.width
      const dx = (event.clientX - dragStart.x) * scl
      const dy = (event.clientY - dragStart.y) * scl
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) state.v1View._dragging = true
      state.v1View.panX = dragStart.px + dx
      state.v1View.panY = dragStart.py + dy
      redraw()
    })
    window.addEventListener('mouseup', () => {
      if (dragStart) {
        dragStart = null
        canvas.style.cursor = state.v1View.scale > 1 ? 'grab' : 'crosshair'
        setTimeout(() => { state.v1View._dragging = false }, 50)
      }
    })
    canvas.addEventListener('dblclick', () => {
      state.v1View.scale = 1
      state.v1View.panX = 0
      state.v1View.panY = 0
      state.v1View._dragging = false
      redraw()
    })
  }

  const legend = document.getElementById('v1-legend')
  if (legend) {
    const steps = 12
    const gradient = Array.from({ length: steps }, (_, index) => viridis(index / (steps - 1))).join(',')
    legend.innerHTML = `<div class="map-legend"><span>${minS.toFixed(2)}</span><div class="legend-bar" style="background:linear-gradient(to right,${gradient})"></div><span>${maxS.toFixed(2)}</span><span style="margin-left:12px;color:#94a3b8;font-size:10px">scroll to zoom · drag to pan · dblclick to reset</span></div>`
  }
}

function renderV2() {
  const scores = [...state.scoreMap.values()].filter((value) => value != null).map(Number)
  if (!scores.length) {
    showNA('v2')
    return
  }
  const bins = 20
  const min = Math.min(...scores)
  const max = Math.max(...scores)
  const binSize = (max - min) / bins || 1
  const counts = Array(bins).fill(0)
  scores.forEach((score) => {
    const index = Math.min(Math.floor((score - min) / binSize), bins - 1)
    counts[index] += 1
  })
  const labels = Array.from({ length: bins }, (_, index) => (min + (index + 0.5) * binSize).toFixed(2))
  const sorted = [...scores].sort((a, b) => a - b)
  const quantile = (prob) => sorted[Math.floor(prob * sorted.length)] ?? 0
  destroyChart('v2')
  const canvas = document.getElementById('v2-canvas')
  if (!canvas) return
  charts.v2 = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: { labels, datasets: [{ data: counts, backgroundColor: Array.from({ length: bins }, (_, index) => viridis(index / bins)), borderWidth: 0 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
      },
    },
  })
  const info = document.getElementById('v2-info')
  if (info) info.innerHTML = `<span>Q1: ${quantile(0.25).toFixed(3)}</span><span>Median: ${quantile(0.5).toFixed(3)}</span><span>Q3: ${quantile(0.75).toFixed(3)}</span><span>n = ${scores.length}</span>`
}

function renderV4() {
  const captureEfficiency = state.evalReport.capture_efficiency
  if (!captureEfficiency) {
    showNA('v4')
    return
  }
  const fractions = captureEfficiency.x_volume_fraction || captureEfficiency.fraction || []
  const captures = captureEfficiency.y_captured_fraction || captureEfficiency.capture || []
  if (!fractions.length) {
    showNA('v4', 'No capture efficiency data')
    return
  }
  destroyChart('v4')
  const canvas = document.getElementById('v4-canvas')
  if (!canvas) return
  charts.v4 = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels: fractions.map((fraction) => Number(fraction).toFixed(2)),
      datasets: [
        { label: 'Capture Efficiency', data: captures, borderColor: '#059669', backgroundColor: 'rgba(5,150,105,0.08)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 },
        { label: 'Random baseline', data: fractions.map(Number), borderColor: '#94a3b8', borderDash: [4, 4], pointRadius: 0, fill: false },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#64748b', boxWidth: 12, font: { size: 11 } } } },
      scales: {
        x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' }, min: 0, max: 1 },
      },
    },
  })
  const xStar = captureEfficiency.x_star
  const info = document.getElementById('v4-info')
  if (info && xStar != null) info.textContent = `x* = ${Number(xStar).toFixed(3)}`
}

function renderV5() {
  const data = state.globalImportance
  if (!data.length) {
    showNA('v5')
    return
  }
  const keys = Object.keys(data[0])
  const featureCol = keys.find((key) => ['feature', 'Feature', 'name'].includes(key)) || keys[0]
  const importanceCol = keys.find((key) => ['importance', 'Importance', 'score', 'gain', 'mean'].includes(key)) || keys[1]
  const groupCol = keys.find((key) => ['group', 'Group', 'category', 'type'].includes(key))
  const sorted = [...data].sort((a, b) => Math.abs(b[importanceCol] || 0) - Math.abs(a[importanceCol] || 0)).slice(0, 20)
  const groups = groupCol ? [...new Set(sorted.map((item) => item[groupCol]).filter(Boolean))] : []
  const colors = {}
  groups.forEach((group, index) => { colors[group] = catColor(index) })
  const legend = document.getElementById('v5-legend')
  if (legend && groups.length) legend.innerHTML = groups.map((group) => `<span class="legend-item"><span class="legend-dot" style="background:${colors[group]}"></span>${group}</span>`).join('')
  destroyChart('v5')
  const canvas = document.getElementById('v5-canvas')
  if (!canvas) return
  charts.v5 = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: {
      labels: sorted.map((item) => item[featureCol]),
      datasets: [{ data: sorted.map((item) => item[importanceCol]), backgroundColor: sorted.map((item) => groupCol && item[groupCol] ? colors[item[groupCol]] : '#2563eb'), borderWidth: 0 }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
        y: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#f1f5f9' } },
      },
    },
  })
}

function renderV6() {
  let data = state.ale
  const container = document.getElementById('v6-body')
  if (!data.length || !container) {
    showNA('v6', 'ALE data not available')
    return
  }
  const rawKeys = Object.keys(data[0])
  if (rawKeys.includes('bin_left') && rawKeys.includes('bin_right') && !rawKeys.includes('bin_center')) {
    data = data.map((row) => ({ ...row, bin_center: ((row.bin_left ?? 0) + (row.bin_right ?? 0)) / 2 }))
  }
  const keys = Object.keys(data[0])
  const featureCol = keys.find((key) => ['feature', 'Feature'].includes(key)) || keys[0]
  const xCol = keys.find((key) => ['x', 'X', 'bin', 'bin_center', 'value'].includes(key)) || keys[1]
  const aleCol = keys.find((key) => ['ale', 'ALE', 'effect', 'y'].includes(key)) || keys[2]
  const groups = {}
  data.forEach((row) => {
    const feature = row[featureCol]
    if (!feature) return
    if (!groups[feature]) groups[feature] = []
    groups[feature].push(row)
  })
  let features = Object.keys(groups)
  if (state.globalImportance.length) {
    const importanceKeys = Object.keys(state.globalImportance[0])
    const featureKey = importanceKeys.find((key) => ['feature', 'Feature'].includes(key)) || importanceKeys[0]
    const importanceKey = importanceKeys.find((key) => ['importance', 'Importance', 'mean'].includes(key)) || importanceKeys[1]
    const importanceMap = new Map(state.globalImportance.map((item) => [item[featureKey], Math.abs(item[importanceKey] || 0)]))
    features.sort((a, b) => (importanceMap.get(b) || 0) - (importanceMap.get(a) || 0))
  }
  features = features.slice(0, 8)
  container.innerHTML = '<div class="ale-grid" id="v6-grid"></div>'
  const grid = document.getElementById('v6-grid')
  features.forEach((feature, index) => {
    const rows = groups[feature].slice().sort((a, b) => (a[xCol] || 0) - (b[xCol] || 0))
    const canvasId = `v6c${index}`
    const cell = document.createElement('div')
    cell.className = 'ale-cell'
    cell.innerHTML = `<div class="ale-title" title="${feature}">${feature}</div><div class="ale-canvas-wrap"><canvas id="${canvasId}"></canvas></div>`
    grid.appendChild(cell)
    requestAnimationFrame(() => {
      const canvas = document.getElementById(canvasId)
      if (!canvas) return
      destroyChart(`v6_${index}`)
      charts[`v6_${index}`] = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { labels: rows.map((row) => row[xCol] != null ? Number(row[xCol]).toFixed(2) : ''), datasets: [{ data: rows.map((row) => row[aleCol]), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,0.07)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { maxTicksLimit: 4, color: '#94a3b8', font: { size: 9 } }, grid: { color: '#f1f5f9' } },
            y: { ticks: { maxTicksLimit: 4, color: '#94a3b8', font: { size: 9 } }, grid: { color: '#f1f5f9' } },
          },
        },
      })
    })
  })
}

function renderV7(blockId) {
  const placeholder = document.getElementById('v7-placeholder')
  const chartWrap = document.getElementById('v7-chart-wrap')
  const title = document.getElementById('v7-title')
  if (title) title.textContent = `SHAP Explanation — Block ${blockId}`
  const all = state.shapValues
  if (!all.length) {
    if (placeholder) {
      placeholder.style.display = 'block'
      placeholder.textContent = 'SHAP data not available'
    }
    return
  }
  const row = all.find((item) => String(item.block_id) === String(blockId))
  if (!row) {
    if (placeholder) {
      placeholder.style.display = 'block'
      placeholder.textContent = `No SHAP data for block ${blockId}`
    }
    if (chartWrap) chartWrap.style.display = 'none'
    return
  }
  if (placeholder) placeholder.style.display = 'none'
  if (chartWrap) chartWrap.style.display = 'block'
  const entries = Object.entries(row)
    .filter(([key]) => key !== 'block_id')
    .map(([feature, value]) => ({ feature, shap: value ?? 0 }))
    .sort((a, b) => Math.abs(b.shap) - Math.abs(a.shap))
    .slice(0, 15)
  destroyChart('v7')
  const canvas = document.getElementById('v7-canvas')
  if (!canvas) return
  charts.v7 = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: { labels: entries.map((item) => item.feature), datasets: [{ data: entries.map((item) => item.shap), backgroundColor: entries.map((item) => item.shap >= 0 ? 'rgba(5,150,105,0.75)' : 'rgba(220,38,38,0.75)'), borderWidth: 0 }] },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
        y: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#f1f5f9' } },
      },
    },
  })
}

function renderV8() {
  if (!state.blocks.length || !state.driverMap.size) {
    showNA('v8')
    return
  }
  const canvas = document.getElementById('v8-canvas')
  if (canvas) canvas.style.display = 'block'
  const groups = [...new Set([...state.driverMap.values()].filter(Boolean))]
  const colors = {}
  groups.forEach((group, index) => { colors[group] = catColor(index) })
  drawChoropleth('v8-canvas', null, (blockId) => {
    const group = state.driverMap.get(blockId)
    return group ? colors[group] : '#e2e8f0'
  }, null)
  const legend = document.getElementById('v8-legend')
  if (legend) legend.innerHTML = groups.map((group) => `<span class="legend-item"><span class="legend-dot" style="background:${colors[group]}"></span>${group}</span>`).join('')
}

function renderV10() {
  const rawData = state.shapGeoUnit
  const container = document.getElementById('v10-body')
  if (!rawData.length || !container) {
    showNA('v10')
    return
  }
  const rawKeys = Object.keys(rawData[0])
  const geoColRaw = rawKeys.find((key) => ['geo_unit', 'minerag_unit', 'geo', 'unit', 'region', 'area', 'zone'].includes(key))
  const isWide = geoColRaw && !rawKeys.find((key) => ['feature', 'Feature', 'feature_name'].includes(key))
  let data = rawData
  if (isWide) {
    const skip = new Set(['summary_type', geoColRaw, 'minerag_unit'])
    data = []
    rawData.forEach((row) => {
      const geoLabel = row.minerag_unit != null && row.minerag_unit !== '' ? `${row[geoColRaw]} / ${row.minerag_unit}` : String(row[geoColRaw])
      Object.entries(row).forEach(([key, value]) => {
        if (!skip.has(key) && value != null && value !== '' && !Number.isNaN(Number(value))) data.push({ geo_unit: geoLabel, feature: key, mean_shap: parseFloat(value) })
      })
    })
  }
  if (!data.length) {
    showNA('v10')
    return
  }
  const keys = Object.keys(data[0])
  const geoCol = keys.find((key) => ['geo_unit', 'geo', 'unit', 'region', 'area', 'zone'].includes(key)) || keys[0]
  const featCol = keys.find((key) => ['feature', 'Feature', 'feature_name'].includes(key)) || keys[1]
  const valCol = keys.find((key) => ['mean_shap', 'shap', 'value', 'mean', 'mean_val'].includes(key)) || keys[2]
  const geos = [...new Set(data.map((item) => item[geoCol]).filter((value) => value != null))]
  const features = [...new Set(data.map((item) => item[featCol]).filter((value) => value != null))]
  const featureImportance = {}
  data.forEach((item) => {
    const feature = item[featCol]
    if (!featureImportance[feature]) featureImportance[feature] = 0
    featureImportance[feature] += Math.abs(item[valCol] || 0)
  })
  const topFeatures = features.sort((a, b) => (featureImportance[b] || 0) - (featureImportance[a] || 0)).slice(0, 30)
  const matrix = {}
  data.forEach((item) => {
    if (!matrix[item[geoCol]]) matrix[item[geoCol]] = {}
    matrix[item[geoCol]][item[featCol]] = item[valCol]
  })
  const values = data.map((item) => item[valCol]).filter((value) => value != null)
  const absMax = Math.max(...values.map(Math.abs)) || 1
  const cellW = Math.max(16, Math.min(40, Math.floor(800 / topFeatures.length)))
  const cellH = Math.max(18, Math.min(32, Math.floor(500 / geos.length)))
  const left = 150
  const top = 70
  const width = left + topFeatures.length * cellW + 20
  const height = top + geos.length * cellH + 50
  const canvas = document.createElement('canvas')
  canvas.width = width
  canvas.height = height
  canvas.style.maxWidth = '100%'
  container.innerHTML = ''
  container.appendChild(canvas)
  const ctx = canvas.getContext('2d')
  ctx.fillStyle = '#f8fafc'
  ctx.fillRect(0, 0, width, height)
  ctx.fillStyle = '#64748b'
  ctx.font = `${Math.min(11, cellW + 2)}px sans-serif`
  topFeatures.forEach((feature, index) => {
    ctx.save()
    ctx.translate(left + index * cellW + cellW / 2, top - 6)
    ctx.rotate(-Math.PI / 4)
    ctx.textAlign = 'left'
    ctx.fillText(String(feature).slice(0, 22), 0, 0)
    ctx.restore()
  })
  ctx.font = '11px sans-serif'
  ctx.textAlign = 'right'
  geos.forEach((geo, index) => {
    ctx.fillStyle = '#475569'
    ctx.fillText(String(geo).slice(0, 20), left - 4, top + index * cellH + cellH / 2 + 4)
  })
  geos.forEach((geo, rowIndex) => {
    topFeatures.forEach((feature, columnIndex) => {
      const value = matrix[geo]?.[feature] ?? 0
      ctx.fillStyle = diverging(value / absMax)
      ctx.fillRect(left + columnIndex * cellW, top + rowIndex * cellH, cellW - 1, cellH - 1)
    })
  })
  const scaleWidth = 120
  for (let index = 0; index < scaleWidth; index += 1) {
    ctx.fillStyle = diverging((index / scaleWidth) * 2 - 1)
    ctx.fillRect(left + index, top + geos.length * cellH + 14, 1, 10)
  }
  ctx.fillStyle = '#64748b'
  ctx.font = '10px sans-serif'
  ctx.textAlign = 'left'
  ctx.fillText(`−${absMax.toFixed(2)}`, left, top + geos.length * cellH + 38)
  ctx.textAlign = 'center'
  ctx.fillText('0', left + scaleWidth / 2, top + geos.length * cellH + 38)
  ctx.textAlign = 'right'
  ctx.fillText(`+${absMax.toFixed(2)}`, left + scaleWidth, top + geos.length * cellH + 38)
}

async function main() {
  const params = new URLSearchParams(window.location.search)
  state.projectId = params.get('project_id')
  if (!state.projectId) {
    document.getElementById('app').innerHTML = '<div style="text-align:center;padding:80px;color:#dc2626;font-size:16px">No project_id in URL</div>'
    return
  }
  document.getElementById('project-id').textContent = state.projectId
  document.title = `Data Cube — ${state.projectId}`
  await loadAll()
  renderV0()
  renderV3()
  renderV9()
  renderV1()
  renderV2()
  renderV4()
  renderV5()
  renderV6()
  renderV8()
  renderV10()
}

main()
