import React, { Component } from "react"
import { MapContainer, TileLayer, Polygon, Popup, LayerGroup } from "react-leaflet"
import "leaflet/dist/leaflet.css"

const TILE_LAYERS = {
  AGIS: "https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}",
  Satellite: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
}

export default class MapView extends Component {
  state = {
    tileId: "AGIS",
  }

  getCenter(elements) {
    if (!elements || elements.length === 0) return [67.98, 66.74]
    return [elements[0].lat || 0, elements[0].lon || 0]
  }

  // Basic WKT parser for POLYGON and MULTIPOLYGON returning arrays
  // of polygons -> rings -> [lat, lon] points. Keeps parsing simple
  // and tolerant to spaces. Assumes coordinates are in "X Y" (lon lat).
  parseWKTToPolygons(wkt) {
    if (!wkt || typeof wkt !== "string") return null
    const txt = wkt.trim()
    const up = txt.toUpperCase()
    try {
      if (up.startsWith("POLYGON")) {
        // POLYGON ((x y, x y, ...), (innerRing...))
        const inner = txt.replace(/POLYGON\s*\(\s*\(/i, "").replace(/\)\s*\)$/, "")
        // split rings by '),(' separators
        const ringTexts = inner.split(/\)\s*,\s*\(/)
        const rings = ringTexts.map((r) => {
          const clean = r.replace(/^\(+/, "").replace(/\)+$/, "").trim()
          const pts = clean.split(/\s*,\s*/).map((p) => {
            const nums = p.trim().split(/\s+/).map(Number)
            return [nums[1], nums[0]] // [lat, lon]
          })
          return pts
        })
        return [rings]
      }
      if (up.startsWith("MULTIPOLYGON")) {
        // MULTIPOLYGON (((...)), ((...)), ...)
        const body = txt.replace(/MULTIPOLYGON\s*\(\s*\(/i, "").replace(/\)\s*\)$/, "")
        // split polygons by ')), ((' pattern
        const polyTexts = body.split(/\)\s*\)\s*,\s*\(\s*\(/)
        const polygons = polyTexts.map((poly) => {
          const inner = poly.replace(/^\(+/, "").replace(/\)+$/, "").trim()
          const ringTexts = inner.split(/\)\s*,\s*\(/)
          const rings = ringTexts.map((r) => {
            const clean = r.replace(/^\(+/, "").replace(/\)+$/, "").trim()
            const pts = clean.split(/\s*,\s*/).map((p) => {
              const nums = p.trim().split(/\s+/).map(Number)
              return [nums[1], nums[0]]
            })
            return pts
          })
          return rings
        })
        return polygons
      }
    } catch (e) {
      return null
    }
    return null
  }

  render() {
    const { elements = [], selectedFields = [], opacity = 0.9, fieldOpacities = {}, fieldColors = {} } = this.props
    const { tileId } = this.state

    const center = this.getCenter(elements)

    // determine which numeric field to color by (prefer 'score')
    const preferredField = elements.length > 0 && elements[0].properties && "score" in elements[0].properties ? "score" : (selectedFields.length > 0 ? selectedFields[0] : null)

    // collect numeric values for color scaling
    const values = elements.map((el) => {
      const v = preferredField ? el.properties?.[preferredField] : undefined
      const n = v === undefined || v === null ? NaN : Number(v)
      return Number.isFinite(n) ? n : NaN
    }).filter((v) => !Number.isNaN(v))

    const vMin = values.length > 0 ? Math.min(...values) : 0
    const vMax = values.length > 0 ? Math.max(...values) : 1

    const valueToColor = (v) => {
      if (v === undefined || v === null || Number.isNaN(Number(v))) return "#999"
      const n = Number(v)
      const t = vMax === vMin ? 0.5 : Math.max(0, Math.min(1, (n - vMin) / (vMax - vMin)))
      // hue from blue (240) to red (0)
      const hue = Math.round((1 - t) * 240)
      return `hsl(${hue},85%,50%)`
    }

    const metersToDegLat = (m) => m / 111320
    const metersToDegLon = (m, lat) => {
      const latRad = (lat * Math.PI) / 180
      const metersPerDegLon = 111320 * Math.cos(latRad)
      return metersPerDegLon === 0 ? 0 : m / metersPerDegLon
    }

    return (
      <div style={{ height: "100%", width: "100%", display: "flex", flexDirection: "column" }}>
        <div style={{ padding: 6 }}>
          <select value={tileId} onChange={(e) => this.setState({ tileId: e.target.value })}>
            <option value="AGIS">AGIS</option>
            <option value="Satellite">Satellite</option>
          </select>
          <span style={{ marginLeft: 10 }}>{elements.length} elements</span>
        </div>

        <div style={{ flex: 1 }}>
          <MapContainer
            key={tileId} // ← главный стабилизатор против ошибки
            center={center}
            zoom={10}
            style={{ height: "100%", width: "100%" }}
          >
            <TileLayer url={TILE_LAYERS[tileId]} opacity={opacity} />

            {/* Render one layer per selected field. Each layer contains polygons from `properties.polygon` colored by that field's values. */}
            {selectedFields && selectedFields.length > 0 ? (
              selectedFields.map((field) => {
                // compute min/max for this field across elements
                const vals = elements.map((el) => {
                  const v = el.properties?.[field]
                  const n = v === undefined || v === null ? NaN : Number(v)
                  return Number.isFinite(n) ? n : NaN
                }).filter((v) => !Number.isNaN(v))
                const minV = vals.length > 0 ? Math.min(...vals) : 0
                const maxV = vals.length > 0 ? Math.max(...vals) : 1

                const valueToColorField = (v) => {
                  if (v === undefined || v === null || Number.isNaN(Number(v))) return "#999"
                  const n = Number(v)
                  const t = maxV === minV ? 0.5 : Math.max(0, Math.min(1, (n - minV) / (maxV - minV)))
                  // if custom colors provided for the field, interpolate between them
                  const colors = fieldColors?.[field]
                    if (colors && colors.start && colors.end) {
                      const interp = (a, b, t) => Math.round(a + (b - a) * t)
                      const hexToRgb = (hex) => {
                        const h = hex.replace('#', '')
                        const full = h.length === 3 ? h.split('').map(c => c + c).join('') : h
                        const bigint = parseInt(full, 16)
                        return { r: (bigint >> 16) & 255, g: (bigint >> 8) & 255, b: bigint & 255 }
                      }
                      const rgbToHex = (r, g, b) => '#' + [r, g, b].map((n) => n.toString(16).padStart(2, '0')).join('')
                      const s = hexToRgb(colors.start)
                      const c = hexToRgb(colors.center ?? colors.start)
                      const e = hexToRgb(colors.end)
                      if (t <= 0.5) {
                        const tt = t * 2
                        const rr = interp(s.r, c.r, tt)
                        const rg = interp(s.g, c.g, tt)
                        const rb = interp(s.b, c.b, tt)
                        return rgbToHex(rr, rg, rb)
                      } else {
                        const tt = (t - 0.5) * 2
                        const rr = interp(c.r, e.r, tt)
                        const rg = interp(c.g, e.g, tt)
                        const rb = interp(c.b, e.b, tt)
                        return rgbToHex(rr, rg, rb)
                      }
                    }
                  // fallback: hue ramp
                  const hue = Math.round((1 - t) * 240)
                  return `hsl(${hue},85%,50%)`
                }

                return (
                  <LayerGroup key={`layer-${field}`}>
                    {elements.map((el) => {
                      const wkt = el.properties?.polygon
                      if (!wkt) return null
                      const polygons = this.parseWKTToPolygons(wkt)
                      if (!polygons) return null
                      const value = el.properties?.[field]
                      const fill = valueToColorField(value)
                      const stroke = "#222"
                      // per-layer opacity: prefer per-field opacity, fall back to default 0.9
                      const layerOpacity = fieldOpacities?.[field] ?? 0.9

                      return polygons.map((rings, pi) => (
                        <React.Fragment key={`${el.id}-${field}-${pi}`}>
                          {rings.map((ring, ri) => (
                            <Polygon key={`${el.id}-${field}-${pi}-ring-${ri}`} positions={ring} pathOptions={{ stroke: false, fillColor: fill, fillOpacity: layerOpacity }}>
                              <Popup>
                                <div>
                                  <b>{field}</b>: {String(value ?? "")}
                                </div>
                                {selectedFields.length > 0 && (
                                  <div>
                                    {selectedFields.map((k) => (
                                      <div key={k}>
                                        <b>{k}</b>: {String(el.properties?.[k] ?? "")}
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </Popup>
                            </Polygon>
                          ))}
                        </React.Fragment>
                      ))
                    })}
                  </LayerGroup>
                )
              })
            ) : null}
          </MapContainer>
        </div>
      </div>
    )
  }
}