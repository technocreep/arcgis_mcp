'use strict'

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
    projectId: null,
    blocks: [],
    scores: [],
    evalReport: {},
    modelMeta: {},
    runMeta: {},
    globalImportance: [],
    ale: [],
    shapValues: [],
    dominantDriver: [],
    shapGeoUnit: [],
    // Derived lookup maps
    blockMap: new Map(),   // block_id -> block row
    scoreMap: new Map(),   // block_id -> score value
    driverMap: new Map(),  // block_id -> dominant_group
}

const charts = {}  // Chart.js instances keyed by id

// ─── CSV / JSON helpers ───────────────────────────────────────────────────────
function splitCSVLine(line) {
    const res = []
    let cur = '', inQ = false
    for (let i = 0; i < line.length; i++) {
        const c = line[i]
        if (c === '"') { inQ = !inQ }
        else if (c === ',' && !inQ) { res.push(cur); cur = '' }
        else cur += c
    }
    res.push(cur)
    return res
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
            if (v === '' || v === 'nan' || v === 'NaN' || v === 'None') {
                row[h] = null
            } else {
                row[h] = isNaN(v) ? v : parseFloat(v)
            }
        })
        return row
    })
}

async function fetchText(path) {
    try {
        const r = await fetch(`/api/projects/${state.projectId}/datacube/files/${path}`)
        return r.ok ? r.text() : null
    } catch { return null }
}

async function fetchJSON(path) {
    try {
        const r = await fetch(`/api/projects/${state.projectId}/datacube/files/${path}`)
        return r.ok ? r.json() : {}
    } catch { return {} }
}

// ─── Color utilities ──────────────────────────────────────────────────────────
function viridis(t) {
    const stops = [
        [68, 1, 84], [62, 74, 137], [33, 145, 140], [94, 201, 98], [253, 231, 37],
    ]
    const s = Math.min(Math.max(t, 0), 1) * (stops.length - 1)
    const lo = Math.floor(s)
    const hi = Math.min(lo + 1, stops.length - 1)
    const f = s - lo
    const c = stops[lo].map((v, i) => Math.round(v + f * (stops[hi][i] - v)))
    return `rgb(${c[0]},${c[1]},${c[2]})`
}

// Diverging colormap: blue ← 0 → red  (t in [-1, 1])
function diverging(t) {
    const clamped = Math.min(Math.max(t, -1), 1)
    if (clamped < 0) {
        const s = -clamped
        return `rgb(${Math.round(255 * (1 - s))},${Math.round(255 * (1 - s))},255)`
    }
    return `rgb(255,${Math.round(255 * (1 - clamped))},${Math.round(255 * (1 - clamped))})`
}

const CAT_PALETTE = [
    '#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
    '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
]
function catColor(i) { return CAT_PALETTE[i % CAT_PALETTE.length] }

// ─── Column detection helpers ─────────────────────────────────────────────────
function detectIdCol(rows) {
    if (!rows || !rows.length) return 'block_id'
    const keys = Object.keys(rows[0])
    return keys.find(k => ['block_id', 'id', 'ID', 'BLOCK_ID'].includes(k)) || keys[0]
}

function detectGeomCols(row) {
    const keys = Object.keys(row)
    const find = (...names) => keys.find(k => names.map(n => n.toLowerCase()).includes(k.toLowerCase()))
    return {
        xmin: find('xmin', 'x_min', 'left', 'lon_min'),
        ymin: find('ymin', 'y_min', 'bottom', 'lat_min'),
        xmax: find('xmax', 'x_max', 'right', 'lon_max'),
        ymax: find('ymax', 'y_max', 'top', 'lat_max'),
        xc: find('x_center', 'xcenter', 'x', 'lon', 'longitude', 'cx'),
        yc: find('y_center', 'ycenter', 'y', 'lat', 'latitude', 'cy'),
    }
}

// ─── DOM helpers ──────────────────────────────────────────────────────────────
function setBody(id, html) {
    const el = document.getElementById(id + '-body') || document.getElementById(id)
    if (el) el.innerHTML = html
}

function showNA(bodyId, msg) {
    setBody(bodyId, `<div class="na-msg">${msg || 'Data not available'}</div>`)
}

function destroyChart(key) {
    if (charts[key]) { charts[key].destroy(); delete charts[key] }
}

// ─── Data loading ─────────────────────────────────────────────────────────────
async function loadAll() {
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
    state.scores = parseCSV(scoresTxt)
    state.evalReport = evalRep || {}
    state.modelMeta = modelMeta || {}
    state.runMeta = runMeta || {}
    state.globalImportance = parseCSV(importTxt)
    state.ale = parseCSV(aleTxt)
    state.shapValues = parseCSV(shapTxt)
    state.dominantDriver = parseCSV(driverTxt)
    state.shapGeoUnit = parseCSV(geoShapTxt)

    // Build lookup maps
    const idCol = detectIdCol(state.blocks)
    state.blocks.forEach(b => state.blockMap.set(String(b[idCol]), b))

    if (state.scores.length) {
        const sIdCol = detectIdCol(state.scores)
        const sKeys = Object.keys(state.scores[0])
        const scoreCol = sKeys.find(k => k !== sIdCol && !['id','block_id','ID'].includes(k)) || sKeys[1]
        state.scores.forEach(s => state.scoreMap.set(String(s[sIdCol]), s[scoreCol]))
    }

    if (state.dominantDriver.length) {
        const dIdCol = detectIdCol(state.dominantDriver)
        const dKeys = Object.keys(state.dominantDriver[0])
        const grpCol = dKeys.find(k => k !== dIdCol) || dKeys[1]
        state.dominantDriver.forEach(d => state.driverMap.set(String(d[dIdCol]), d[grpCol]))
    }
}

// ─── Canvas choropleth ────────────────────────────────────────────────────────
function drawChoropleth(canvasId, colorFn, onClickBlock) {
    const canvas = document.getElementById(canvasId)
    if (!canvas || !state.blocks.length) return

    const ctx = canvas.getContext('2d')
    const W = canvas.width, H = canvas.height
    ctx.clearRect(0, 0, W, H)
    ctx.fillStyle = '#0f172a'
    ctx.fillRect(0, 0, W, H)

    if (!state.blocks.length) return

    const geom = detectGeomCols(state.blocks[0])
    const idCol = detectIdCol(state.blocks)
    const useBox = geom.xmin && geom.ymin && geom.xmax && geom.ymax

    let xs, ys, xe, ye

    if (useBox) {
        xs = state.blocks.map(b => b[geom.xmin])
        ys = state.blocks.map(b => b[geom.ymin])
        xe = state.blocks.map(b => b[geom.xmax])
        ye = state.blocks.map(b => b[geom.ymax])
    } else if (geom.xc && geom.yc) {
        const cxArr = state.blocks.map(b => b[geom.xc]).filter(v => v != null).sort((a, b) => a - b)
        const diffs = cxArr.slice(1).map((v, i) => v - cxArr[i]).filter(d => d > 0).sort((a, b) => a - b)
        const step = diffs[Math.floor(diffs.length / 2)] || 1000
        xs = state.blocks.map(b => b[geom.xc] - step / 2)
        ys = state.blocks.map(b => b[geom.yc] - step / 2)
        xe = state.blocks.map(b => b[geom.xc] + step / 2)
        ye = state.blocks.map(b => b[geom.yc] + step / 2)
    } else {
        ctx.fillStyle = '#475569'
        ctx.font = '14px sans-serif'
        ctx.textAlign = 'center'
        ctx.fillText('Cannot detect geometry columns in blocks.csv', W / 2, H / 2)
        return
    }

    const valid = (arr) => arr.filter(v => v != null && isFinite(v))
    const minX = Math.min(...valid(xs))
    const minY = Math.min(...valid(ys))
    const maxX = Math.max(...valid(xe))
    const maxY = Math.max(...valid(ye))
    const rangeX = (maxX - minX) || 1
    const rangeY = (maxY - minY) || 1

    // Maintain aspect ratio with padding
    const PAD = 10
    const mapAspect = rangeX / rangeY
    const canvasAspect = W / H
    let drawW = W - 2 * PAD, drawH = H - 2 * PAD, offX = PAD, offY = PAD

    if (mapAspect > canvasAspect) {
        drawH = drawW / mapAspect
        offY = (H - drawH) / 2
    } else {
        drawW = drawH * mapAspect
        offX = (W - drawW) / 2
    }

    const toX = x => offX + ((x - minX) / rangeX) * drawW
    const toY = y => offY + ((maxY - y) / rangeY) * drawH  // flip Y

    // Draw blocks
    const blockRects = []
    state.blocks.forEach((b, idx) => {
        if (xs[idx] == null) return
        const x1 = toX(xs[idx])
        const y1 = toY(ye[idx])
        const bw = Math.max(toX(xe[idx]) - x1, 1)
        const bh = Math.max(toY(ys[idx]) - y1, 1)
        const bid = String(b[idCol])
        ctx.fillStyle = colorFn(bid) || '#334155'
        ctx.fillRect(x1, y1, bw, bh)
        blockRects.push({ id: bid, x: x1, y: y1, w: bw, h: bh })
    })

    // Subtle grid
    ctx.strokeStyle = 'rgba(255,255,255,0.06)'
    ctx.lineWidth = 0.5
    blockRects.forEach(({ x, y, w, h }) => ctx.strokeRect(x, y, w, h))

    // Click handler
    if (onClickBlock) {
        canvas.style.cursor = 'crosshair'
        canvas.onclick = (e) => {
            const rect = canvas.getBoundingClientRect()
            const scale = canvas.width / rect.width
            const mx = (e.clientX - rect.left) * scale
            const my = (e.clientY - rect.top) * scale
            for (const br of blockRects) {
                if (mx >= br.x && mx <= br.x + br.w && my >= br.y && my <= br.y + br.h) {
                    // Highlight selected block
                    ctx.strokeStyle = '#ffffff'
                    ctx.lineWidth = 2
                    ctx.strokeRect(br.x, br.y, br.w, br.h)
                    onClickBlock(br.id)
                    break
                }
            }
        }
    }
}

// ─── V3: Model Metrics ────────────────────────────────────────────────────────
function renderV3() {
    const r = state.evalReport
    const m = state.modelMeta

    const xStar = r['x*'] ?? r.x_star ?? r.threshold ?? null
    const metrics = [
        { label: 'PR-AUC', value: r.pr_auc != null ? r.pr_auc.toFixed(3) : 'N/A', icon: '◉', accent: 'emerald' },
        { label: 'CV PR-AUC', value: r.cv_pr_auc != null ? r.cv_pr_auc.toFixed(3) : 'N/A', icon: '◎', accent: 'blue' },
        { label: 'Threshold x*', value: xStar != null ? xStar.toFixed(3) : 'N/A', icon: '◈', accent: 'violet' },
        { label: 'Model', value: m.model_type || r.model_type || 'N/A', icon: '⬡', accent: 'amber' },
    ]

    setBody('v3', `
        <div class="metric-grid">
            ${metrics.map(({ label, value, icon, accent }) => `
            <div class="metric-card accent-${accent}">
                <div class="metric-icon">${icon}</div>
                <div class="metric-value">${value}</div>
                <div class="metric-label">${label}</div>
            </div>`).join('')}
        </div>`)
}

// ─── V9: Run Parameters ───────────────────────────────────────────────────────
function renderV9() {
    const meta = state.runMeta
    if (!Object.keys(meta).length) { showNA('v9'); return }

    const rows = Object.entries(meta).map(([k, v]) =>
        `<tr>
            <td class="meta-key">${k}</td>
            <td class="meta-val">${Array.isArray(v) ? v.join(', ') : String(v ?? '')}</td>
        </tr>`).join('')

    setBody('v9', `<table class="meta-table"><tbody>${rows}</tbody></table>`)
}

// ─── V1: Prospectivity Score Map ──────────────────────────────────────────────
function renderV1() {
    if (!state.blocks.length) { showNA('v1', 'No blocks.csv data'); return }

    const scoreVals = [...state.scoreMap.values()].filter(v => v != null)
    if (!scoreVals.length) { showNA('v1', 'No scores data'); return }

    const minS = Math.min(...scoreVals)
    const maxS = Math.max(...scoreVals)
    const range = (maxS - minS) || 1

    drawChoropleth('v1-canvas', (bid) => {
        const s = state.scoreMap.get(bid)
        return s != null ? viridis((s - minS) / range) : '#1e293b'
    }, (bid) => renderV7(bid))

    // Legend
    const legend = document.getElementById('v1-legend')
    if (!legend) return
    const steps = 12
    const grad = Array.from({ length: steps }, (_, i) => viridis(i / (steps - 1))).join(',')
    legend.innerHTML = `
        <div class="map-legend">
            <span>${minS.toFixed(2)}</span>
            <div class="legend-bar" style="background:linear-gradient(to right,${grad})"></div>
            <span>${maxS.toFixed(2)}</span>
        </div>`
}

// ─── V2: Score Distribution Histogram ────────────────────────────────────────
function renderV2() {
    const scores = [...state.scoreMap.values()].filter(v => v != null)
    if (!scores.length) { showNA('v2'); return }

    const BINS = 20
    const min = Math.min(...scores), max = Math.max(...scores)
    const binSize = (max - min) / BINS || 1
    const counts = Array(BINS).fill(0)
    scores.forEach(s => {
        const i = Math.min(Math.floor((s - min) / binSize), BINS - 1)
        counts[i]++
    })
    const labels = Array.from({ length: BINS }, (_, i) => (min + (i + 0.5) * binSize).toFixed(2))

    // Quartiles
    const sorted = [...scores].sort((a, b) => a - b)
    const q = (p) => sorted[Math.floor(p * sorted.length)] ?? 0

    destroyChart('v2')
    const canvas = document.getElementById('v2-canvas')
    if (!canvas) return
    charts.v2 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: counts,
                backgroundColor: Array.from({ length: BINS }, (_, i) => viridis(i / BINS)),
                borderWidth: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' } },
            },
        },
    })

    const info = document.getElementById('v2-info')
    if (info) info.innerHTML = `
        <span>Q1: ${q(0.25).toFixed(3)}</span>
        <span>Median: ${q(0.5).toFixed(3)}</span>
        <span>Q3: ${q(0.75).toFixed(3)}</span>
        <span>n = ${scores.length}</span>`
}

// ─── V4: Capture Efficiency Curve ────────────────────────────────────────────
function renderV4() {
    const ce = state.evalReport.capture_efficiency
    if (!ce) { showNA('v4'); return }

    let fractions, captures
    if (Array.isArray(ce)) {
        if (ce.length && Array.isArray(ce[0])) {
            fractions = ce.map(p => p[0])
            captures = ce.map(p => p[1])
        } else if (ce.length && typeof ce[0] === 'object') {
            const keys = Object.keys(ce[0])
            fractions = ce.map(p => p[keys[0]])
            captures = ce.map(p => p[keys[1]])
        } else {
            showNA('v4', 'Unknown capture_efficiency format')
            return
        }
    } else if (ce.fraction && ce.capture) {
        fractions = ce.fraction
        captures = ce.capture
    } else {
        showNA('v4', 'Unknown capture_efficiency format')
        return
    }

    const r = state.evalReport
    const xStar = r['x*'] ?? r.x_star ?? r.threshold

    destroyChart('v4')
    const canvas = document.getElementById('v4-canvas')
    if (!canvas) return
    charts.v4 = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: fractions.map(f => Number(f).toFixed(2)),
            datasets: [
                {
                    label: 'Capture Efficiency',
                    data: captures,
                    borderColor: '#34d399',
                    backgroundColor: 'rgba(52,211,153,0.1)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                },
                {
                    label: 'Random baseline',
                    data: fractions.map(Number),
                    borderColor: '#475569',
                    borderDash: [4, 4],
                    pointRadius: 0,
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { labels: { color: '#94a3b8', boxWidth: 12 } } },
            scales: {
                x: { ticks: { maxTicksLimit: 6, color: '#94a3b8' }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' }, min: 0, max: 1 },
            },
        },
    })

    const info = document.getElementById('v4-info')
    if (info && xStar != null) info.textContent = `x* = ${Number(xStar).toFixed(3)}`
}

// ─── V5: Feature Importance ───────────────────────────────────────────────────
function renderV5() {
    const data = state.globalImportance
    if (!data.length) { showNA('v5'); return }

    const keys = Object.keys(data[0])
    const featureCol = keys.find(k => ['feature', 'Feature', 'name', 'feature_name'].includes(k)) || keys[0]
    const importCol = keys.find(k => ['importance', 'Importance', 'score', 'value', 'gain'].includes(k)) || keys[1]
    const groupCol = keys.find(k => ['group', 'Group', 'category', 'type', 'group_name'].includes(k))

    const sorted = [...data]
        .sort((a, b) => Math.abs(b[importCol] ?? 0) - Math.abs(a[importCol] ?? 0))
        .slice(0, 20)

    const groups = groupCol ? [...new Set(sorted.map(d => d[groupCol]).filter(Boolean))] : []
    const groupColor = {}
    groups.forEach((g, i) => { groupColor[g] = catColor(i) })

    destroyChart('v5')
    const canvas = document.getElementById('v5-canvas')
    if (!canvas) return
    charts.v5 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: sorted.map(d => d[featureCol]),
            datasets: [{
                data: sorted.map(d => d[importCol]),
                backgroundColor: sorted.map(d => groupCol && d[groupCol] ? groupColor[d[groupCol]] : '#60a5fa'),
                borderWidth: 0,
            }],
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#94a3b8', font: { size: 11 } }, grid: { color: '#1e293b' } },
            },
        },
    })

    // Group legend
    if (groups.length) {
        const legend = document.getElementById('v5-legend')
        if (legend) {
            legend.innerHTML = groups.map(g =>
                `<span class="legend-item">
                    <span class="legend-dot" style="background:${groupColor[g]}"></span>${g}
                </span>`).join('')
        }
    }
}

// ─── V6: ALE Plots ────────────────────────────────────────────────────────────
function renderV6() {
    const data = state.ale
    const container = document.getElementById('v6-grid')
    if (!data.length || !container) { showNA('v6', 'ALE data not available'); return }

    const keys = Object.keys(data[0])
    const featureCol = keys.find(k => ['feature', 'Feature', 'FEATURE', 'feature_name'].includes(k)) || keys[0]
    const xCol = keys.find(k => ['x', 'X', 'bin', 'value', 'bin_center'].includes(k)) || keys[1]
    const aleCol = keys.find(k => ['ale', 'ALE', 'effect', 'y', 'ale_effect'].includes(k)) || keys[2]

    // Group by feature
    const groups = {}
    data.forEach(row => {
        const f = row[featureCol]
        if (f == null) return
        if (!groups[f]) groups[f] = []
        groups[f].push(row)
    })

    // Order features by global importance
    let features = Object.keys(groups)
    if (state.globalImportance.length) {
        const gKeys = Object.keys(state.globalImportance[0])
        const gFCol = gKeys.find(k => ['feature', 'Feature'].includes(k)) || gKeys[0]
        const gICol = gKeys.find(k => ['importance', 'Importance'].includes(k)) || gKeys[1]
        const imp = new Map(state.globalImportance.map(d => [d[gFCol], Math.abs(d[gICol] ?? 0)]))
        features.sort((a, b) => (imp.get(b) || 0) - (imp.get(a) || 0))
    }
    features = features.slice(0, 8)

    container.innerHTML = ''

    features.forEach((f, idx) => {
        const rows = groups[f].slice().sort((a, b) => (a[xCol] ?? 0) - (b[xCol] ?? 0))
        const canvasId = `v6-chart-${idx}`
        const cell = document.createElement('div')
        cell.className = 'ale-cell'
        cell.innerHTML = `
            <div class="ale-title" title="${f}">${f}</div>
            <div class="ale-canvas-wrap"><canvas id="${canvasId}"></canvas></div>`
        container.appendChild(cell)

        requestAnimationFrame(() => {
            const canvas = document.getElementById(canvasId)
            if (!canvas) return
            destroyChart(`v6_${idx}`)
            charts[`v6_${idx}`] = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: {
                    labels: rows.map(r => r[xCol] != null ? Number(r[xCol]).toFixed(2) : ''),
                    datasets: [{
                        data: rows.map(r => r[aleCol]),
                        borderColor: '#60a5fa',
                        backgroundColor: 'rgba(96,165,250,0.1)',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { ticks: { maxTicksLimit: 4, color: '#64748b', font: { size: 9 } }, grid: { color: '#1e293b' } },
                        y: { ticks: { maxTicksLimit: 4, color: '#64748b', font: { size: 9 } }, grid: { color: '#1e293b' } },
                    },
                },
            })
        })
    })
}

// ─── V7: SHAP Waterfall (drilldown) ──────────────────────────────────────────
function renderV7(blockId) {
    document.getElementById('v7-placeholder').style.display = 'none'
    document.getElementById('v7-chart-wrap').style.display = 'block'
    document.getElementById('v7-title').textContent = `SHAP Explanation — Block ${blockId}`

    const all = state.shapValues
    if (!all.length) {
        document.getElementById('v7-chart-wrap').innerHTML = '<div class="na-msg">SHAP data not available</div>'
        return
    }

    const keys = Object.keys(all[0])
    const idCol = keys.find(k => ['block_id', 'id', 'ID', 'BLOCK_ID'].includes(k)) || keys[0]
    const featureCol = keys.find(k => ['feature', 'Feature', 'feature_name'].includes(k)) || keys[1]
    const shapCol = keys.find(k => ['shap_value', 'shap', 'value', 'SHAP', 'shap_val'].includes(k)) || keys[2]

    const blockRows = all.filter(r => String(r[idCol]) === String(blockId))
    if (!blockRows.length) {
        document.getElementById('v7-chart-wrap').innerHTML = `<div class="na-msg">No SHAP data for block ${blockId}</div>`
        return
    }

    const sorted = [...blockRows]
        .sort((a, b) => Math.abs(b[shapCol] ?? 0) - Math.abs(a[shapCol] ?? 0))
        .slice(0, 15)

    destroyChart('v7')
    const canvas = document.getElementById('v7-canvas')
    if (!canvas) return
    charts.v7 = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: sorted.map(d => d[featureCol]),
            datasets: [{
                data: sorted.map(d => d[shapCol]),
                backgroundColor: sorted.map(d =>
                    (d[shapCol] ?? 0) >= 0 ? 'rgba(52,211,153,0.8)' : 'rgba(248,113,113,0.8)'),
                borderWidth: 0,
            }],
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#94a3b8', font: { size: 11 } }, grid: { color: '#1e293b' } },
            },
        },
    })
}

// ─── V8: Dominant Driver Map ──────────────────────────────────────────────────
function renderV8() {
    if (!state.blocks.length || !state.driverMap.size) { showNA('v8'); return }

    const groups = [...new Set([...state.driverMap.values()].filter(Boolean))]
    const groupColor = {}
    groups.forEach((g, i) => { groupColor[g] = catColor(i) })

    drawChoropleth('v8-canvas', (bid) => {
        const g = state.driverMap.get(bid)
        return g ? groupColor[g] : '#1e293b'
    }, null)

    const legend = document.getElementById('v8-legend')
    if (legend) {
        legend.innerHTML = groups.map(g =>
            `<span class="legend-item">
                <span class="legend-dot" style="background:${groupColor[g]}"></span>${g}
            </span>`).join('')
    }
}

// ─── V10: SHAP Heatmap by Geo-Unit ───────────────────────────────────────────
function renderV10() {
    const data = state.shapGeoUnit
    const container = document.getElementById('v10-body')
    if (!data.length || !container) { showNA('v10'); return }

    const keys = Object.keys(data[0])
    const geoCol = keys.find(k => ['geo_unit', 'geo', 'unit', 'region', 'area', 'zone'].includes(k)) || keys[0]
    const featureCol = keys.find(k => ['feature', 'Feature', 'feature_name'].includes(k)) || keys[1]
    const valCol = keys.find(k => ['mean_shap', 'shap', 'value', 'mean', 'mean_val'].includes(k)) || keys[2]

    const geos = [...new Set(data.map(d => d[geoCol]).filter(v => v != null))]
    const features = [...new Set(data.map(d => d[featureCol]).filter(v => v != null))]

    // If too many features, take top N by mean |shap|
    const featImport = {}
    data.forEach(d => {
        const f = d[featureCol]
        if (!featImport[f]) featImport[f] = 0
        featImport[f] += Math.abs(d[valCol] ?? 0)
    })
    const topFeatures = features
        .sort((a, b) => (featImport[b] || 0) - (featImport[a] || 0))
        .slice(0, 30)

    // Build matrix
    const matrix = {}
    data.forEach(d => {
        const g = d[geoCol], f = d[featureCol]
        if (!matrix[g]) matrix[g] = {}
        matrix[g][f] = d[valCol]
    })

    const vals = data.map(d => d[valCol]).filter(v => v != null)
    const absMax = Math.max(...vals.map(Math.abs)) || 1

    const CELL_W = Math.max(14, Math.min(36, Math.floor(800 / topFeatures.length)))
    const CELL_H = Math.max(14, Math.min(28, Math.floor(500 / geos.length)))
    const LEFT = 140, TOP = 70

    const W = LEFT + topFeatures.length * CELL_W + 20
    const H = TOP + geos.length * CELL_H + 50

    const canvas = document.createElement('canvas')
    canvas.width = W
    canvas.height = H
    canvas.style.maxWidth = '100%'
    container.innerHTML = ''
    container.appendChild(canvas)

    const ctx = canvas.getContext('2d')
    ctx.fillStyle = '#0f172a'
    ctx.fillRect(0, 0, W, H)

    // Feature labels (rotated, top)
    ctx.fillStyle = '#94a3b8'
    ctx.font = `${Math.min(11, CELL_W + 2)}px sans-serif`
    topFeatures.forEach((f, j) => {
        ctx.save()
        ctx.translate(LEFT + j * CELL_W + CELL_W / 2, TOP - 6)
        ctx.rotate(-Math.PI / 4)
        ctx.textAlign = 'left'
        ctx.fillText(String(f).slice(0, 20), 0, 0)
        ctx.restore()
    })

    // Geo-unit labels (left)
    ctx.font = '11px sans-serif'
    ctx.textAlign = 'right'
    geos.forEach((g, i) => {
        ctx.fillStyle = '#94a3b8'
        ctx.fillText(String(g).slice(0, 18), LEFT - 4, TOP + i * CELL_H + CELL_H / 2 + 4)
    })

    // Cells
    geos.forEach((g, i) => {
        topFeatures.forEach((f, j) => {
            const v = matrix[g]?.[f] ?? 0
            ctx.fillStyle = diverging(v / absMax)
            ctx.fillRect(LEFT + j * CELL_W, TOP + i * CELL_H, CELL_W - 1, CELL_H - 1)
        })
    })

    // Color scale
    const SCALE_W = 120
    for (let i = 0; i < SCALE_W; i++) {
        ctx.fillStyle = diverging((i / SCALE_W) * 2 - 1)
        ctx.fillRect(LEFT + i, TOP + geos.length * CELL_H + 16, 1, 10)
    }
    ctx.fillStyle = '#64748b'
    ctx.font = '10px sans-serif'
    ctx.textAlign = 'left'
    ctx.fillText(`−${absMax.toFixed(2)}`, LEFT, TOP + geos.length * CELL_H + 40)
    ctx.textAlign = 'center'
    ctx.fillText('0', LEFT + SCALE_W / 2, TOP + geos.length * CELL_H + 40)
    ctx.textAlign = 'right'
    ctx.fillText(`+${absMax.toFixed(2)}`, LEFT + SCALE_W, TOP + geos.length * CELL_H + 40)
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    const params = new URLSearchParams(window.location.search)
    state.projectId = params.get('project_id')

    if (!state.projectId) {
        document.getElementById('app').innerHTML =
            '<div style="text-align:center;padding:80px;color:#f87171;font-size:16px">No project_id specified in URL</div>'
        return
    }

    document.getElementById('project-id').textContent = state.projectId
    document.title = `Data Cube — ${state.projectId}`

    await loadAll()

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
