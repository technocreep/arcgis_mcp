export default class PointItem {
  constructor({ id = null, lat = null, lon = null, properties = {} } = {}) {
    this.id = id
    this.lat = lat !== null ? Number(lat) : null
    this.lon = lon !== null ? Number(lon) : null
    this.properties = properties || {}
  }

  // convenience factory from raw API item
  static fromRaw(item, idx = 0) {
    const id = item.id || `e${idx}`
    const lat = item.lat !== undefined ? Number(item.lat) : null
    const lon = item.lon !== undefined ? Number(item.lon) : null
    const props = Object.fromEntries(
      Object.entries(item).filter(([k]) => k !== "lat" && k !== "lon" && k !== "id")
    )
    return new PointItem({ id, lat, lon, properties: props })
  }

  toGeoJSON() {
    return {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [this.lon, this.lat],
      },
      properties: { id: this.id, ...this.properties },
    }
  }
}
