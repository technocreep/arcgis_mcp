import React from "react"
import MapView from "./MapView"
import PointItem from "../models/PointItem"


export default class MainPage extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      folder: "output_lekyn",
      elements: [],
      availableFields: [],
      selectedFields: [],
      loading: false,
      error: null,
      // per-field opacity map: { fieldName: opacity }
      fieldOpacities: {},
      // per-field gradient colors: { fieldName: { start: '#0000ff', end: '#ff0000' } }
      fieldColors: {},
    }
    this.loadData = this.loadData.bind(this)
    this.onFolderChange = this.onFolderChange.bind(this)
    this.onFieldOpacityChange = this.onFieldOpacityChange.bind(this)
  }

  onFolderChange(e) {
    this.setState({ folder: e.target.value })
  }

  async loadData() {
    this.setState({ loading: true, error: null })
    try {
      const res = await fetch("/api/dataAPI/get_current_data", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ folder_name: this.state.folder }),
      })
      const j = await res.json()
      if (j.status !== 0) {
        this.setState({ error: j.message || "API returned error", elements: [] })
      } else {
        const raw = j.data || []
        // convert raw items into PointItem instances
        const standardized = raw.map((item, idx) => PointItem.fromRaw(item, idx))
          // compute available property keys (exclude id/lat/lon)
          const fieldSet = new Set()
          standardized.forEach((p) => {
            const keys = Object.keys(p.properties || {})
            keys.forEach((k) => fieldSet.add(k))
          })
          const availableFields = Array.from(fieldSet)
          // exclude internal/non-display fields
          fieldSet.delete("polygon")
          const availableFieldsFiltered = Array.from(fieldSet)

          // initialize or merge per-field opacity values (preserve existing where possible)
          const nextOpacities = { ...(this.state.fieldOpacities || {}) }
          const nextColors = { ...(this.state.fieldColors || {}) }
          availableFieldsFiltered.forEach((f) => {
            if (nextOpacities[f] === undefined) nextOpacities[f] = 0.9
            if (nextColors[f] === undefined) nextColors[f] = { start: "#2166ac", center: "#f4a582", end: "#b2182b" } // blue->warm->red
          })

          this.setState({
            elements: standardized,
            availableFields: availableFieldsFiltered,
            selectedFields: availableFieldsFiltered.slice(),
            fieldOpacities: nextOpacities,
            fieldColors: nextColors,
          })
      }
    } catch (e) {
      this.setState({ error: String(e), elements: [] })
    } finally {
      this.setState({ loading: false })
    }
  }

  /* global opacity removed: per-field opacities are used instead */

  onFieldOpacityChange(field, e) {
    const v = Number(e.target.value)
    this.setState((s) => ({ fieldOpacities: { ...(s.fieldOpacities || {}), [field]: v } }))
  }

  onFieldColorChange(field, which, e) {
    const v = e.target.value
    this.setState((s) => ({ fieldColors: { ...(s.fieldColors || {}), [field]: { ...(s.fieldColors?.[field] || {}), [which]: v } } }))
  }

  unselectAll() {
    this.setState({ selectedFields: [] })
  }

  render() {
    const { folder, elements, loading, error, availableFields, selectedFields } = this.state
    return (
      <div style={{ padding: 12 }}>
        <h1>Data Dashboard — Map Preview</h1>
        <div style={{ marginBottom: 8 }}>
          <label>
            Folder name:&nbsp;
            <input value={folder} onChange={this.onFolderChange} />
          </label>
          <button onClick={this.loadData} style={{ marginLeft: 8 }} disabled={loading}>
            {loading ? "Loading..." : "Load"}
          </button>
          <p>Loaded {elements.length} elements</p>
        </div>

        {error && <div style={{ color: "red" }}>{error}</div>}
        <div style={{ display: "flex", gap: 12 }}>
          <div style={{ width: 220, display: "flex", flexDirection: "column", alignItems: "flex-start", textAlign: "left" }}>
            <div style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <h3 style={{ margin: 0 }}>Fields</h3>
              <button onClick={() => this.unselectAll()} style={{ fontSize: 12 }}>Unselect all</button>
            </div>
            {/* per-field opacity controls shown next to each field */}
            {availableFields.length === 0 && <div>No fields available</div>}
            <div style={{ width: "100%", overflowY: "auto", maxHeight: "calc(100vh - 140px)", paddingRight: 6 }}>
              {availableFields.map((f) => (
                <div key={f} style={{ marginBottom: 10, width: "100%" }}>
                <div>
                  <label style={{ cursor: "pointer" }}>
                    <input
                      type="checkbox"
                      checked={selectedFields.includes(f)}
                      onChange={() => {
                        const next = selectedFields.includes(f)
                          ? selectedFields.filter((s) => s !== f)
                          : [...selectedFields, f]
                        this.setState({ selectedFields: next })
                      }}
                    />
                    &nbsp;{f}
                  </label>
                </div>
                <div style={{ marginTop: 6, display: "flex", alignItems: "center", gap: 8 }}>
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.05"
                    value={this.state.fieldOpacities?.[f] ?? 0.9}
                    onChange={(e) => this.onFieldOpacityChange(f, e)}
                    style={{ flex: 1 }}
                  />
                  <div style={{ width: 48, textAlign: "right", fontSize: 12 }}>{Math.round((this.state.fieldOpacities?.[f] ?? 0.9) * 100)}%</div>
                </div>
                <div style={{ marginTop: 6, display: "flex", gap: 8, alignItems: "center", width: "100%" }}>
                  {/* Gradient swatch with three stops */}
                  <div
                    style={{
                      flex: 1,
                      height: 20,
                      borderRadius: 4,
                      border: "1px solid #ccc",
                      background: `linear-gradient(90deg, ${this.state.fieldColors?.[f]?.start ?? "#2166ac"} 0%, ${this.state.fieldColors?.[f]?.center ?? "#f4a582"} 50%, ${this.state.fieldColors?.[f]?.end ?? "#b2182b"} 100%)`,
                    }}
                    title="Gradient preview"
                  />
                  {/* compact color pickers for start/center/end */}
                  <input
                    type="color"
                    value={this.state.fieldColors?.[f]?.start ?? "#2166ac"}
                    onChange={(e) => this.onFieldColorChange(f, "start", e)}
                    style={{ width: 28, height: 28, padding: 0, border: "none", background: "transparent" }}
                    aria-label={`${f} start color`}
                  />
                  <input
                    type="color"
                    value={this.state.fieldColors?.[f]?.center ?? "#f4a582"}
                    onChange={(e) => this.onFieldColorChange(f, "center", e)}
                    style={{ width: 28, height: 28, padding: 0, border: "none", background: "transparent" }}
                    aria-label={`${f} center color`}
                  />
                  <input
                    type="color"
                    value={this.state.fieldColors?.[f]?.end ?? "#b2182b"}
                    onChange={(e) => this.onFieldColorChange(f, "end", e)}
                    style={{ width: 28, height: 28, padding: 0, border: "none", background: "transparent" }}
                    aria-label={`${f} end color`}
                  />
                </div>
              </div>
              ))}
            </div>
          </div>

            <div style={{ flex: 1, height: "100vh", width: "100%", border: "1px solid #ccc" }}>
            <MapView elements={elements} selectedFields={selectedFields} fieldOpacities={this.state.fieldOpacities} fieldColors={this.state.fieldColors} />
          </div>
        </div>


      </div>
    )
  }
}
