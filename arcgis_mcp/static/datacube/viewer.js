'use strict'

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
    projectId: null,
    files: new Set(),
    blocks: [],
    evalReport: {},
    modelMeta: {},
    runMeta: {},
    globalImportance: [],
    ale: [],
    shapValues: [],    // wide format: block_id + feature cols
    dominantDriver: [],
    shapGeoUnit: [],
    // Derived
    scoreMap: new Map(),   // block_id -> score
    driverMap: new Map(),  // block_id -> dominant_group
    v1Rects: [],           // canvas hit-test rects [{id, x, y, w, h}]
    v1SelectedId: null,
    v1View: { scale: 1, panX: 0, panY: 0 },  // zoom/pan transform
}

const charts = {}

// ─── CSV / JSON helpers ───────────────────────────────────────────────────────
function splitCSVLine(line) {
    const res = []; let cur = '', inQ = false
    for (let i = 0; i < line.length; i++) {
        const c = line[i]
        if (c === '"') { inQ = !inQ }
        else if (c === ',' && !inQ) { res.push(cur); cur = '' }
        else cur += c
    }
    res.push(cur); return res
}

function parseCSV(text) {
    if (!text) return []
    const lines = text.trim().split(/\r?\n/)
    if (lines.length < 2) return []
    const headers = splitCSVLine(lines[0]).map(h => h.replace(/^"|"$/g, '').trim())
    return lines.slice(1).filter(l => l.trim()).map(line => {
        const vals = splitCSVLine(line)
        const row = {}
        headers.forEach((h, i) => {
            const v = (vals[i] || '').replace(/^"|"$/g, '').trim()
            row[h] = (v === '' || v === 'nan' || v === 'NaN' || v === 'None') ? null
                   : (isNaN(v) ? v : parseFloat(v))
        })
        return row
    })
}

function fileUrl(path) {
    return `/api/projects/${state.projectId}/datacube/files/${path}`
}

function authHeader() {
    const u = localStorage.getItem('gis_auth_user')
    const p = localStorage.getItem('gis_auth_pass')
    if (!u || !p) return null
    return 'Basic ' + btoa(u + ':' + p)
}

function authOpts() {
    const h = authHeader()
    return h ? { headers: { Authorization: h } } : {}
}

async function fetchText(path) {
    try { const r = await fetch(fileUrl(path), authOpts()); return r.ok ? r.text() : null }
    catch { return null }
}

async function fetchJSON(path) {
    try { const r = await fetch(fileUrl(path), authOpts()); return r.ok ? r.json() : {} }
    catch { return {} }
}

// ─── Color utilities ──────────────────────────────────────────────────────────
function viridis(t) {
    const stops = [[68,1,84],[62,74,137],[33,145,140],[94,201,98],[253,231,37]]
    const s = Math.min(Math.max(t, 0), 1) * (stops.length - 1)
    const lo = Math.floor(s), hi = Math.min(lo + 1, stops.length - 1), f = s - lo
    const c = stops[lo].map((v, i) => Math.round(v + f * (stops[hi][i] - v)))
    return `rgb(${c[0]},${c[1]},${c[2]})`
}

function diverging(t) {  // t in [-1,1]
    const clamped = Math.min(Math.max(t, -1), 1)
    if (clamped < 0) { const s = -clamped; return `rgb(${Math.round(255*(1-s))},${Math.round(255*(1-s))},255)` }
    return `rgb(255,${Math.round(255*(1-clamped))},${Math.round(255*(1-clamped))})`
}

const CAT_PALETTE = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac']
function catColor(i) { return CAT_PALETTE[i % CAT_PALETTE.length] }

// ─── DOM helpers ──────────────────────────────────────────────────────────────
function setBody(id, html) {
    const el = document.getElementById(id + '-body') || document.getElementById(id)
    if (el) el.innerHTML = html
}
function showNA(bodyId, msg) { setBody(bodyId, `<div class="na-msg">${msg || 'Data not available'}</div>`) }
function destroyChart(key) { if (charts[key]) { charts[key].destroy(); delete charts[key] } }

// ─── Data loading ─────────────────────────────────────────────────────────────
async function loadAll() {
    // Get file list first
    try {
        const r = await fetch(`/api/projects/${state.projectId}/datacube`, authOpts())
        if (r.ok) { const d = await r.json(); state.files = new Set(d.files || []) }
    } catch {}

    const [
        blocksTxt, scoresTxt, evalRep, modelMeta, runMeta,
        importTxt, aleTxt, shapTxt, driverTxt, geoShapTxt,
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
    ])

    state.blocks = parseCSV(blocksTxt)
    state.evalReport = evalRep || {}
    state.modelMeta = modelMeta || {}
    state.runMeta = runMeta || {}
    state.globalImportance = parseCSV(importTxt)
    state.ale = parseCSV(aleTxt)
    state.shapValues = parseCSV(shapTxt)
    state.dominantDriver = parseCSV(driverTxt)
    state.shapGeoUnit = parseCSV(geoShapTxt)

    // Score map from scores.csv
    const scores = parseCSV(scoresTxt)
    scores.forEach(s => state.scoreMap.set(String(s['block_id'] ?? s[Object.keys(s)[0]]), s['score'] ?? s[Object.keys(s)[1]]))

    // Driver map
    state.dominantDriver.forEach(d => {
        const keys = Object.keys(d)
        const idCol = keys.find(k => k === 'block_id') || keys[0]
        const grpCol = keys.find(k => k !== idCol) || keys[1]
        state.driverMap.set(String(d[idCol]), d[grpCol])
    })
}

// ─── Canvas choropleth ────────────────────────────────────────────────────────
// view = { scale, panX, panY } — zoom/pan (optional, used for v1-canvas)
function drawChoropleth(canvasId, tooltipId, colorFn, onClickBlock, view) {
    const canvas = document.getElementById(canvasId)
    if (!canvas || !state.blocks.length) return

    const ctx = canvas.getContext('2d')
    const W = canvas.width, H = canvas.height
    ctx.clearRect(0, 0, W, H)
    ctx.fillStyle = '#f8fafc'
    ctx.fillRect(0, 0, W, H)

    // Detect coordinate columns — prefer metric coords (x_m/y_m) with cell_size_m
    const sample = state.blocks[0]
    const keys = Object.keys(sample)
    const hasMetric = keys.includes('x_m') && keys.includes('y_m')
    const hasCellSize = keys.includes('cell_size_m')

    let xs, ys, xe, ye

    if (hasMetric && hasCellSize) {
        const half = sample.cell_size_m / 2
        xs = state.blocks.map(b => b.x_m - half)
        ys = state.blocks.map(b => b.y_m - half)
        xe = state.blocks.map(b => b.x_m + half)
        ye = state.blocks.map(b => b.y_m + half)
    } else {
        // Fallback: detect centroid columns and estimate step
        const xcol = keys.find(k => ['x_m','lon','longitude','x','cx','x_center'].includes(k))
        const ycol = keys.find(k => ['y_m','lat','latitude','y','cy','y_center'].includes(k))
        if (!xcol || !ycol) {
            ctx.fillStyle = '#64748b'; ctx.font = '14px sans-serif'; ctx.textAlign = 'center'
            ctx.fillText('Cannot detect geometry columns', W/2, H/2)
            return
        }
        const cxArr = [...new Set(state.blocks.map(b => b[xcol]))].sort((a,b) => a-b)
        const diffs = cxArr.slice(1).map((v,i) => v - cxArr[i]).filter(d => d > 0).sort((a,b) => a-b)
        const step = diffs[Math.floor(diffs.length/2)] || 5000
        xs = state.blocks.map(b => b[xcol] - step/2)
        ys = state.blocks.map(b => b[ycol] - step/2)
        xe = state.blocks.map(b => b[xcol] + step/2)
        ye = state.blocks.map(b => b[ycol] + step/2)
    }

    const valid = arr => arr.filter(v => v != null && isFinite(v))
    const minX = Math.min(...valid(xs)), maxX = Math.max(...valid(xe))
    const minY = Math.min(...valid(ys)), maxY = Math.max(...valid(ye))
    const rangeX = (maxX - minX) || 1, rangeY = (maxY - minY) || 1

    // Maintain aspect ratio
    const PAD = 12
    const mapAspect = rangeX / rangeY, canvasAspect = W / H
    let drawW = W - 2*PAD, drawH = H - 2*PAD, offX = PAD, offY = PAD
    if (mapAspect > canvasAspect) { drawH = drawW / mapAspect; offY = (H - drawH) / 2 }
    else { drawW = drawH * mapAspect; offX = (W - drawW) / 2 }

    // Base coordinate mapping (north-up: larger y_m → top of canvas)
    const baseToX = x => offX + ((x - minX) / rangeX) * drawW
    const baseToY = y => offY + ((maxY - y) / rangeY) * drawH

    // Apply zoom/pan around canvas centre
    const sc = view?.scale || 1, px = view?.panX || 0, py = view?.panY || 0
    const cx = W / 2, cy = H / 2
    const toX = x => (baseToX(x) - cx) * sc + cx + px
    const toY = y => (baseToY(y) - cy) * sc + cy + py

    const idCol = keys.find(k => k === 'block_id') || keys[0]
    const rects = []

    state.blocks.forEach((b, idx) => {
        if (xs[idx] == null) return
        const x1 = toX(xs[idx]), y1 = toY(ye[idx])
        const bw = Math.max(toX(xe[idx]) - x1, 2)
        const bh = Math.max(toY(ys[idx]) - y1, 2)
        const bid = String(b[idCol])
        ctx.fillStyle = colorFn(bid) || '#e2e8f0'
        ctx.fillRect(x1, y1, bw, bh)
        ctx.strokeStyle = 'rgba(255,255,255,0.4)'
        ctx.lineWidth = 0.5
        ctx.strokeRect(x1, y1, bw, bh)
        rects.push({ id: bid, x: x1, y: y1, w: bw, h: bh })
    })

    // Store rects for re-highlighting
    if (canvasId === 'v1-canvas') state.v1Rects = rects

    // Highlight selected
    function highlightBlock(bid, color, lineW) {
        const r = rects.find(r => r.id === bid)
        if (!r) return
        ctx.strokeStyle = color
        ctx.lineWidth = lineW
        ctx.strokeRect(r.x + 0.5, r.y + 0.5, r.w - 1, r.h - 1)
    }

    if (state.v1SelectedId && canvasId === 'v1-canvas') {
        highlightBlock(state.v1SelectedId, '#0ea5e9', 3)
    }

    // Click handler
    if (onClickBlock) {
        canvas.style.cursor = 'crosshair'
        canvas.onclick = (e) => {
            const rect = canvas.getBoundingClientRect()
            const scl = canvas.width / rect.width
            const mx = (e.clientX - rect.left) * scl
            const my = (e.clientY - rect.top) * scl
            for (const br of rects) {
                if (mx >= br.x && mx <= br.x + br.w && my >= br.y && my <= br.y + br.h) {
                    state.v1SelectedId = br.id
                    drawChoropleth(canvasId, tooltipId, colorFn, onClickBlock, view)
                    onClickBlock(br.id)
                    break
                }
            }
        }

        // Hover tooltip
        const tooltip = tooltipId ? document.getElementById(tooltipId) : null
        canvas.onmousemove = (e) => {
            if (view?._dragging) return
            const rect = canvas.getBoundingClientRect()
            const scl = canvas.width / rect.width
            const mx = (e.clientX - rect.left) * scl
            const my = (e.clientY - rect.top) * scl
            let found = null
            for (const br of rects) {
                if (mx >= br.x && mx <= br.x + br.w && my >= br.y && my <= br.y + br.h) { found = br; break }
            }
            if (tooltip) {
                if (found) {
                    const score = state.scoreMap.get(found.id)
                    tooltip.innerHTML = `<b>${found.id}</b><br>Score: ${score != null ? Number(score).toFixed(3) : '—'}`
                    const cRect = canvas.parentElement.getBoundingClientRect()
                    tooltip.style.left = (e.clientX - cRect.left + 12) + 'px'
                    tooltip.style.top  = (e.clientY - cRect.top  + 12) + 'px'
                    tooltip.style.display = 'block'
                    canvas.style.cursor = 'pointer'
                } else {
                    tooltip.style.display = 'none'
                    canvas.style.cursor = sc > 1 ? 'grab' : 'crosshair'
                }
            }
        }
        canvas.onmouseleave = () => { if (tooltip) tooltip.style.display = 'none' }
    }
}

// ─── V0: Interactive map (iframe) ─────────────────────────────────────────────
async function renderV0() {
    if (!state.files.has('viz/map.html')) { showNA('v0', 'Interactive map (viz/map.html) not found'); return }
    try {
        const r = await fetch(fileUrl('viz/map.html'), authOpts())
        if (!r.ok) { showNA('v0', 'Failed to load map'); return }
        const html = await r.text()
        const blob = new Blob([html], { type: 'text/html' })
        const blobUrl = URL.createObjectURL(blob)
        setBody('v0', `<iframe src="${blobUrl}" class="map-iframe" title="Prospectivity Map"></iframe>`)
    } catch { showNA('v0', 'Failed to load map') }
}

// ─── V3: Model Metrics ────────────────────────────────────────────────────────
function renderV3() {
    const r = state.evalReport
    const m = state.modelMeta
    const ce = r.capture_efficiency || {}

    const fmt = (v) => v != null ? Number(v).toFixed(3) : 'N/A'

    const metrics = [
        { label: 'PR-AUC (test)',   value: fmt(r.metrics?.pr_auc),     icon: '◉', accent: 'emerald' },
        { label: 'CV PR-AUC',       value: fmt(m.cv?.mean_pr_auc),     icon: '◎', accent: 'blue' },
        { label: 'x* (volume)',     value: fmt(ce.x_star),             icon: '◈', accent: 'violet' },
        { label: 'Score @ x*',      value: fmt(ce.score_threshold_at_x_star), icon: '◇', accent: 'amber' },
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
            Model: <b style="color:#475569">${m.model_type || '—'}</b> &nbsp;|&nbsp;
            CV splits: <b style="color:#475569">${m.cv?.effective_splits ?? m.cv?.requested_splits ?? '—'}</b>
        </div>`)
}

// ─── V9: Run Parameters ───────────────────────────────────────────────────────
function renderMetaValue(v, depth) {
    if (v == null) return '<span style="color:#94a3b8">—</span>'
    if (Array.isArray(v)) {
        if (!v.length) return '<span style="color:#94a3b8">[]</span>'
        if (v.every(x => typeof x !== 'object')) return v.join(', ')
        return `<code style="font-size:11px">${JSON.stringify(v)}</code>`
    }
    if (typeof v === 'object') {
        if (depth >= 1) return `<code style="font-size:11px">${JSON.stringify(v)}</code>`
        const inner = Object.entries(v).map(([k, val]) =>
            `<span class="meta-sub-key">${k}</span><span>${renderMetaValue(val, depth + 1)}</span>`
        ).join('')
        return `<div class="meta-sub">${inner}</div>`
    }
    return String(v)
}

function renderV9() {
    const meta = state.runMeta
    if (!Object.keys(meta).length) { showNA('v9'); return }
    const rows = Object.entries(meta).map(([k, v]) =>
        `<tr><td class="meta-key">${k}</td><td class="meta-val">${renderMetaValue(v, 0)}</td></tr>`
    ).join('')
    setBody('v9', `<table class="meta-table"><tbody>${rows}</tbody></table>`)
}

// ─── V1: Prospectivity Score Map (canvas, zoomable) ──────────────────────────
function renderV1() {
    if (!state.blocks.length) { showNA('v1', 'No blocks.csv data'); return }
    const scoreVals = [...state.scoreMap.values()].filter(v => v != null).map(Number)
    if (!scoreVals.length) { showNA('v1', 'No scores data'); return }

    const minS = Math.min(...scoreVals), maxS = Math.max(...scoreVals)
    const range = (maxS - minS) || 1

    const colorFn = (bid) => {
        const s = state.scoreMap.get(bid)
        return s != null ? viridis((Number(s) - minS) / range) : '#e2e8f0'
    }
    const clickFn = (bid) => { renderV7(bid) }

    const v = state.v1View
    drawChoropleth('v1-canvas', 'v1-tooltip', colorFn, clickFn, v)

    // ── Zoom / pan ────────────────────────────────────────────────────────────
    const canvas = document.getElementById('v1-canvas')
    if (!canvas || canvas._zoomBound) {
        // handlers already attached on first render — just update legend and return
    } else {
        canvas._zoomBound = true

        function redraw() {
            drawChoropleth('v1-canvas', 'v1-tooltip', colorFn, clickFn, state.v1View)
        }

        // Wheel → zoom around cursor
        canvas.addEventListener('wheel', (e) => {
            e.preventDefault()
            const zf = e.deltaY < 0 ? 1.25 : 1 / 1.25
            const rect = canvas.getBoundingClientRect()
            const scl = canvas.width / rect.width
            const mx = (e.clientX - rect.left) * scl
            const my = (e.clientY - rect.top) * scl
            const cxc = canvas.width / 2, cyc = canvas.height / 2
            const newScale = Math.max(1, Math.min(16, state.v1View.scale * zf))
            const af = newScale / state.v1View.scale
            state.v1View.panX = (mx - cxc) * (1 - af) + state.v1View.panX * af
            state.v1View.panY = (my - cyc) * (1 - af) + state.v1View.panY * af
            state.v1View.scale = newScale
            if (newScale === 1) { state.v1View.panX = 0; state.v1View.panY = 0 }
            redraw()
        }, { passive: false })

        // Drag → pan (only when zoomed in)
        let dragStart = null
        canvas.addEventListener('mousedown', (e) => {
            if (state.v1View.scale > 1) {
                dragStart = { x: e.clientX, y: e.clientY, px: state.v1View.panX, py: state.v1View.panY }
                state.v1View._dragging = false
                canvas.style.cursor = 'grabbing'
            }
        })
        window.addEventListener('mousemove', (e) => {
            if (!dragStart) return
            const rect = canvas.getBoundingClientRect()
            const scl = canvas.width / rect.width
            const dx = (e.clientX - dragStart.x) * scl
            const dy = (e.clientY - dragStart.y) * scl
            if (Math.abs(dx) > 3 || Math.abs(dy) > 3) state.v1View._dragging = true
            state.v1View.panX = dragStart.px + dx
            state.v1View.panY = dragStart.py + dy
            redraw()
        })
        window.addEventListener('mouseup', () => {
            if (dragStart) {
                dragStart = null
                canvas.style.cursor = state.v1View.scale > 1 ? 'grab' : 'crosshair'
                // brief delay so onclick sees _dragging flag, then clear it
                setTimeout(() => { state.v1View._dragging = false }, 50)
            }
        })

        // Double-click → reset zoom
        canvas.addEventListener('dblclick', () => {
            state.v1View.scale = 1; state.v1View.panX = 0; state.v1View.panY = 0
            state.v1View._dragging = false
            redraw()
        })
    }

    const legend = document.getElementById('v1-legend')
    if (legend) {
        const steps = 12
        const grad = Array.from({ length: steps }, (_, i) => viridis(i / (steps - 1))).join(',')
        legend.innerHTML = `
            <div class="map-legend">
                <span>${minS.toFixed(2)}</span>
                <div class="legend-bar" style="background:linear-gradient(to right,${grad})"></div>
                <span>${maxS.toFixed(2)}</span>
                <span style="margin-left:12px;color:#94a3b8;font-size:10px">scroll to zoom · drag to pan · dblclick to reset</span>
            </div>`
    }
}

// ─── V2: Score Histogram ──────────────────────────────────────────────────────
function renderV2() {
    const scores = [...state.scoreMap.values()].filter(v => v != null).map(Number)
    if (!scores.length) { showNA('v2'); return }

    const BINS = 20, min = Math.min(...scores), max = Math.max(...scores)
    const binSize = (max - min) / BINS || 1
    const counts = Array(BINS).fill(0)
    scores.forEach(s => { const i = Math.min(Math.floor((s - min) / binSize), BINS - 1); counts[i]++ })
    const labels = Array.from({ length: BINS }, (_, i) => (min + (i + 0.5) * binSize).toFixed(2))

    const sorted = [...scores].sort((a, b) => a - b)
    const q = p => sorted[Math.floor(p * sorted.length)] ?? 0

    destroyChart('v2')
    const canvas = document.getElementById('v2-canvas')
    if (!canvas) return
    charts.v2 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [{ data: counts, backgroundColor: Array.from({length: BINS}, (_, i) => viridis(i/BINS)), borderWidth: 0 }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
            },
        },
    })

    const info = document.getElementById('v2-info')
    if (info) info.innerHTML = `<span>Q1: ${q(0.25).toFixed(3)}</span><span>Median: ${q(0.5).toFixed(3)}</span><span>Q3: ${q(0.75).toFixed(3)}</span><span>n = ${scores.length}</span>`
}

// ─── V4: Capture Efficiency Curve ────────────────────────────────────────────
function renderV4() {
    const ce = state.evalReport.capture_efficiency
    if (!ce) { showNA('v4'); return }

    // Field names: x_volume_fraction / y_captured_fraction
    const fractions = ce.x_volume_fraction || ce.fraction || []
    const captures  = ce.y_captured_fraction || ce.capture || []

    if (!fractions.length) { showNA('v4', 'No capture efficiency data'); return }

    destroyChart('v4')
    const canvas = document.getElementById('v4-canvas')
    if (!canvas) return
    charts.v4 = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: fractions.map(f => Number(f).toFixed(2)),
            datasets: [
                { label: 'Capture Efficiency', data: captures, borderColor: '#059669', backgroundColor: 'rgba(5,150,105,0.08)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 },
                { label: 'Random baseline', data: fractions.map(Number), borderColor: '#94a3b8', borderDash: [4,4], pointRadius: 0, fill: false },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { labels: { color: '#64748b', boxWidth: 12, font: { size: 11 } } } },
            scales: {
                x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' }, min: 0, max: 1 },
            },
        },
    })

    const xStar = ce.x_star
    const info = document.getElementById('v4-info')
    if (info && xStar != null) info.textContent = `x* = ${Number(xStar).toFixed(3)}`
}

// ─── V5: Feature Importance (interactive Chart.js) ───────────────────────────
function renderV5() {
    const data = state.globalImportance
    if (!data.length) { showNA('v5'); return }
    const keys = Object.keys(data[0])
    const featureCol = keys.find(k => ['feature','Feature','name'].includes(k)) || keys[0]
    const importCol  = keys.find(k => ['importance','Importance','score','gain'].includes(k)) || keys[1]
    const groupCol   = keys.find(k => ['group','Group','category','type'].includes(k))
    const sorted = [...data].sort((a,b) => Math.abs(b[importCol]||0) - Math.abs(a[importCol]||0)).slice(0,20)
    const groups = groupCol ? [...new Set(sorted.map(d => d[groupCol]).filter(Boolean))] : []
    const gc = {}; groups.forEach((g,i) => { gc[g] = catColor(i) })

    const legend = document.getElementById('v5-legend')
    if (legend && groups.length) {
        legend.innerHTML = groups.map(g => `<span class="legend-item"><span class="legend-dot" style="background:${gc[g]}"></span>${g}</span>`).join('')
    }

    destroyChart('v5')
    const canvas = document.getElementById('v5-canvas')
    if (!canvas) return
    charts.v5 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: sorted.map(d => d[featureCol]),
            datasets: [{ data: sorted.map(d => d[importCol]), backgroundColor: sorted.map(d => groupCol && d[groupCol] ? gc[d[groupCol]] : '#2563eb'), borderWidth: 0 }],
        },
        options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } }, y: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#f1f5f9' } } },
        },
    })
}

// ─── V6: ALE Plots (interactive Chart.js grid) ───────────────────────────────
function renderV6() {
    const data = state.ale
    const container = document.getElementById('v6-body')
    if (!data.length || !container) { showNA('v6', 'ALE data not available'); return }
    const keys = Object.keys(data[0])
    const featureCol = keys.find(k => ['feature','Feature'].includes(k)) || keys[0]
    const xCol = keys.find(k => ['x','X','bin','bin_center','value'].includes(k)) || keys[1]
    const aleCol = keys.find(k => ['ale','ALE','effect','y'].includes(k)) || keys[2]

    const groups = {}
    data.forEach(row => { const f = row[featureCol]; if (f) { if (!groups[f]) groups[f] = []; groups[f].push(row) } })
    let features = Object.keys(groups)
    if (state.globalImportance.length) {
        const gk = Object.keys(state.globalImportance[0])
        const gf = gk.find(k => ['feature','Feature'].includes(k)) || gk[0]
        const gi = gk.find(k => ['importance','Importance'].includes(k)) || gk[1]
        const imp = new Map(state.globalImportance.map(d => [d[gf], Math.abs(d[gi]||0)]))
        features.sort((a,b) => (imp.get(b)||0) - (imp.get(a)||0))
    }
    features = features.slice(0, 8)
    container.innerHTML = `<div class="ale-grid" id="v6-grid"></div>`
    const grid = document.getElementById('v6-grid')

    features.forEach((f, idx) => {
        const rows = groups[f].slice().sort((a,b) => (a[xCol]||0) - (b[xCol]||0))
        const canvasId = `v6c${idx}`
        const cell = document.createElement('div'); cell.className = 'ale-cell'
        cell.innerHTML = `<div class="ale-title" title="${f}">${f}</div><div class="ale-canvas-wrap"><canvas id="${canvasId}"></canvas></div>`
        grid.appendChild(cell)
        requestAnimationFrame(() => {
            const canvas = document.getElementById(canvasId); if (!canvas) return
            destroyChart(`v6_${idx}`)
            charts[`v6_${idx}`] = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels: rows.map(r => r[xCol] != null ? Number(r[xCol]).toFixed(2) : ''), datasets: [{ data: rows.map(r => r[aleCol]), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,0.07)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
                    scales: { x: { ticks: { maxTicksLimit: 4, color: '#94a3b8', font: { size: 9 } }, grid: { color: '#f1f5f9' } }, y: { ticks: { maxTicksLimit: 4, color: '#94a3b8', font: { size: 9 } }, grid: { color: '#f1f5f9' } } } },
            })
        })
    })
}

// ─── V7: SHAP Waterfall (drilldown) ──────────────────────────────────────────
// shap_values.csv is WIDE format: block_id col + one col per feature
function renderV7(blockId) {
    const placeholder = document.getElementById('v7-placeholder')
    const chartWrap   = document.getElementById('v7-chart-wrap')
    const titleEl     = document.getElementById('v7-title')

    if (titleEl) titleEl.textContent = `SHAP Explanation — Block ${blockId}`

    const all = state.shapValues
    if (!all.length) {
        if (placeholder) { placeholder.style.display = 'block'; placeholder.textContent = 'SHAP data not available' }
        return
    }

    // Find the row for this block
    const row = all.find(r => String(r['block_id']) === String(blockId))
    if (!row) {
        if (placeholder) { placeholder.style.display = 'block'; placeholder.textContent = `No SHAP data for block ${blockId}` }
        if (chartWrap) chartWrap.style.display = 'none'
        return
    }

    if (placeholder) placeholder.style.display = 'none'
    if (chartWrap) chartWrap.style.display = 'block'

    // Wide → long: skip block_id column, rest are feature shap values
    const entries = Object.entries(row)
        .filter(([k]) => k !== 'block_id')
        .map(([feature, val]) => ({ feature, shap: val ?? 0 }))
        .sort((a, b) => Math.abs(b.shap) - Math.abs(a.shap))
        .slice(0, 15)

    destroyChart('v7')
    const canvas = document.getElementById('v7-canvas')
    if (!canvas) return
    charts.v7 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: entries.map(d => d.feature),
            datasets: [{ data: entries.map(d => d.shap), backgroundColor: entries.map(d => d.shap >= 0 ? 'rgba(5,150,105,0.75)' : 'rgba(220,38,38,0.75)'), borderWidth: 0 }],
        },
        options: {
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { color: '#94a3b8' }, grid: { color: '#f1f5f9' } },
                y: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#f1f5f9' } },
            },
        },
    })
}

// ─── V8: Dominant Driver Map (interactive canvas) ────────────────────────────
function renderV8() {
    if (!state.blocks.length || !state.driverMap.size) { showNA('v8'); return }
    // Show canvas (hidden by default)
    const canvas = document.getElementById('v8-canvas')
    if (canvas) canvas.style.display = 'block'
    const groups = [...new Set([...state.driverMap.values()].filter(Boolean))]
    const gc = {}; groups.forEach((g,i) => { gc[g] = catColor(i) })
    drawChoropleth('v8-canvas', null, (bid) => { const g = state.driverMap.get(bid); return g ? gc[g] : '#e2e8f0' }, null)
    const legend = document.getElementById('v8-legend')
    if (legend) legend.innerHTML = groups.map(g => `<span class="legend-item"><span class="legend-dot" style="background:${gc[g]}"></span>${g}</span>`).join('')
}

// ─── V10: SHAP Heatmap by Geo-Unit ───────────────────────────────────────────
function renderV10() {
    const data = state.shapGeoUnit
    const container = document.getElementById('v10-body')
    if (!data.length || !container) { showNA('v10'); return }

    const keys = Object.keys(data[0])
    const geoCol  = keys.find(k => ['geo_unit','geo','unit','region','area','zone'].includes(k)) || keys[0]
    const featCol = keys.find(k => ['feature','Feature','feature_name'].includes(k)) || keys[1]
    const valCol  = keys.find(k => ['mean_shap','shap','value','mean','mean_val'].includes(k)) || keys[2]

    const geos     = [...new Set(data.map(d => d[geoCol]).filter(v => v != null))]
    const features = [...new Set(data.map(d => d[featCol]).filter(v => v != null))]

    const featImport = {}
    data.forEach(d => { const f = d[featCol]; if (!featImport[f]) featImport[f] = 0; featImport[f] += Math.abs(d[valCol]||0) })
    const topFeatures = features.sort((a,b) => (featImport[b]||0) - (featImport[a]||0)).slice(0, 30)

    const matrix = {}
    data.forEach(d => { if (!matrix[d[geoCol]]) matrix[d[geoCol]] = {}; matrix[d[geoCol]][d[featCol]] = d[valCol] })

    const vals = data.map(d => d[valCol]).filter(v => v != null)
    const absMax = Math.max(...vals.map(Math.abs)) || 1

    const CELL_W = Math.max(16, Math.min(40, Math.floor(800 / topFeatures.length)))
    const CELL_H = Math.max(18, Math.min(32, Math.floor(500 / geos.length)))
    const LEFT = 150, TOP = 70
    const W = LEFT + topFeatures.length * CELL_W + 20
    const H = TOP + geos.length * CELL_H + 50

    const canvas = document.createElement('canvas')
    canvas.width = W; canvas.height = H; canvas.style.maxWidth = '100%'
    container.innerHTML = ''; container.appendChild(canvas)

    const ctx = canvas.getContext('2d')
    ctx.fillStyle = '#f8fafc'; ctx.fillRect(0, 0, W, H)

    // Feature labels
    ctx.fillStyle = '#64748b'; ctx.font = `${Math.min(11, CELL_W+2)}px sans-serif`
    topFeatures.forEach((f, j) => {
        ctx.save(); ctx.translate(LEFT + j*CELL_W + CELL_W/2, TOP - 6)
        ctx.rotate(-Math.PI/4); ctx.textAlign = 'left'
        ctx.fillText(String(f).slice(0, 22), 0, 0)
        ctx.restore()
    })

    // Geo labels
    ctx.font = '11px sans-serif'; ctx.textAlign = 'right'
    geos.forEach((g, i) => {
        ctx.fillStyle = '#475569'
        ctx.fillText(String(g).slice(0, 20), LEFT - 4, TOP + i*CELL_H + CELL_H/2 + 4)
    })

    // Cells
    geos.forEach((g, i) => {
        topFeatures.forEach((f, j) => {
            const v = matrix[g]?.[f] ?? 0
            ctx.fillStyle = diverging(v / absMax)
            ctx.fillRect(LEFT + j*CELL_W, TOP + i*CELL_H, CELL_W - 1, CELL_H - 1)
        })
    })

    // Colour scale
    const SW = 120
    for (let i = 0; i < SW; i++) { ctx.fillStyle = diverging((i/SW)*2-1); ctx.fillRect(LEFT+i, TOP+geos.length*CELL_H+14, 1, 10) }
    ctx.fillStyle = '#64748b'; ctx.font = '10px sans-serif'
    ctx.textAlign = 'left';   ctx.fillText(`−${absMax.toFixed(2)}`, LEFT, TOP+geos.length*CELL_H+38)
    ctx.textAlign = 'center'; ctx.fillText('0', LEFT+SW/2, TOP+geos.length*CELL_H+38)
    ctx.textAlign = 'right';  ctx.fillText(`+${absMax.toFixed(2)}`, LEFT+SW, TOP+geos.length*CELL_H+38)
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    const params = new URLSearchParams(window.location.search)
    state.projectId = params.get('project_id')
    if (!state.projectId) {
        document.getElementById('app').innerHTML =
            '<div style="text-align:center;padding:80px;color:#dc2626;font-size:16px">No project_id in URL</div>'
        return
    }
    if (!authHeader()) {
        document.getElementById('app').innerHTML =
            '<div style="text-align:center;padding:80px;color:#dc2626;font-size:16px">' +
            'Not authenticated. Please <a href="/ui/" style="color:#2563eb">sign in via the portal</a> first.</div>'
        return
    }
    document.getElementById('project-id').textContent = state.projectId
    document.title = `Data Cube — ${state.projectId}`

    await loadAll()

    renderV0()
    renderV3()
    renderV9()
    renderV1()
    renderV7.__ready = true   // signal that V7 can be shown on click
    renderV2()
    renderV4()
    renderV5()
    renderV6()
    renderV8()
    renderV10()
}

main()
