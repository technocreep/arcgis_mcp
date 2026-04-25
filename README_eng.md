# ArcGIS MCP Server

[🇷🇺 Русский](README.md)

Implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io) over geospatial data from ArcGIS File Geodatabase (`.gdb`) with optional metadata from ArcGIS Pro projects (`.aprx`). An LLM agent connects to the server and receives a set of tools for searching, analyzing, and visualizing geological, geophysical, and cartographic data.

**Tech stack:** Python 3.12, FastAPI, GeoPandas, Fiona, Matplotlib, Folium, Neo4j, MinIO (S3).

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Open WebUI (browser)                                │
│                                                                              │
│  User: "Show gravimetry map for the Lekyn project"                           │
│       │                                                          ▲           │
│       │  LLM decides to call a tool                              │           │
│       ▼                                                          │           │
│  [ LLM agent ] ──── reads /openapi.json ───────────────────────-┘            │
└────────────────────────────┬─────────────────────────────────────────────────┘
                             │  POST /plot_layer
                             │  {"layer": "gravimetry"}
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    gis-mcp  :10002  (api_server/server.py)                  │
│                                                                             │
│  Tools (mcp_server/tools/):                                                 │
│  ┌─────────────┐  ┌────────────┐  ┌───────────┐  ┌──────────┐  ┌────────┐   │
│  │  inventory  │  │   query    │  │    viz    │  │datacube  │  │  KG    │   │
│  │  (P0)       │  │  (P1)      │  │(plot_*)   │  │          │  │ query  │   │
│  └──────┬──────┘  └─────┬──────┘  └─────┬─────┘  └────┬─────┘  └───┬───-┘   │
└─────────┼───────────────┼───────────────┼─────────────┼────────────┼──────-─┘
          │               │               │             │            │
          ▼               ▼               ▼             ▼            ▼
    manifest.json     .gdb (via      .gdb + MinIO    data-cube    Neo4j KG
    (fast, <100ms)    GeoPandas)     (PNG/HTML)      :internal    :7687
                      (1–30 s)       (2–60 s)
                                          │
                                          ▼
                                   MinIO / S3 (gis-viz)
                                   PNG → public URL
                                          │
                             returns markdown link
                                   to the image


─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  Ingestion flow (separate path)  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

┌──────────────────────────────────────────────────────────────────────────────┐
│                   gis-loader  :10003  (ingestion/app.py)                     │
│                                                                              │
│  POST /api/projects/upload  ◄── ZIP (.gdb + .aprx)  ◄── user/UI             │
│                │                                                             │
│                ▼  pipeline.py (7 steps)                                      │
│  1. Unpack ZIP                                                               │
│  2. Locate .aprx / .gdb                                                      │
│  3. Parse .aprx  ──► display_name, groups, CRS                               │
│  4. Parse .gdb   ──► fields, stats, extent, attachments                      │
│  5. Build manifest.json                                                      │
│  6. Quality checks                                                           │
│  7. KG indexing ───────────────────────────────────────► Neo4j KG            │
│                           (pdf_parser → InvestigationCard nodes)             │
│                           (non-blocking: error does not break pipeline)      │
│                │                                                             │
│                ▼                                                             │
│  PROJECTS_DIR/{project_id}/manifest.json  ◄── shared Docker volume          │
└──────────────────────────────────────────────────────────────────────────────┘

              ▲ shared volume "projects" ▼
   gis-mcp reads the same files written by gis-loader


─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  KG query (geo_context_query)  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

  User: "Which organizations conducted gold surveys in Lekyn?"
       │
       ▼
  gis-mcp: geo_context_query(query)
       │
       ▼
  nl_to_cypher.py ──► vLLM (KG_LLM_MODEL) ──► Cypher query
       │
       ▼
  Neo4j  MATCH (ic:InvestigationCard)-[:TARGETS]->(m:Mineral {name:"gold"})
         MATCH (ic)-[:CONDUCTED_BY]->(o:Organization)
         RETURN o.name, ic.year_start, ic.title  LIMIT 50
       │
       ▼
  JSON result ──► agent formats response for the user
```

---

## Quick Start

```bash
# 1. Copy template and fill in variables
cp .env.template .env

# 2. Start the stack
docker compose up -d

# 3. Open
# Ingestion UI: http://localhost:10003/ui/
# MCP Swagger:  http://localhost:10002/docs
# Neo4j:        http://localhost:7474
```

**Docker services:**

| Service | Port | Purpose |
|---------|------|---------|
| `gis-loader` | 10003 | Ingestion API — upload `.aprx`/`.gdb` ZIP, run pipeline, KG indexing |
| `gis-mcp` | 10002 | OpenAPI Tool Server — tools for Open WebUI agent |
| `neo4j` | 7474 / 7687 | Knowledge Graph (Community Edition) |
| `data-cube` | — | ML prospectivity pipeline (`Dockerfile.datacube`) |

**Make commands:**

```bash
make rebuild       # Full rebuild of the entire stack
make rebuild-app   # Rebuild only gis-loader + gis-mcp
make reload-app    # Restart app containers without rebuild (code edits)
make rebuild-cube  # Rebuild data-cube from scratch
make update-cube   # git pull + pip install inside data-cube container
```

---

## Architecture

```
arcgis_mcp/
├── config.py                     # Global configuration (env vars)
├── mcp_server/
│   ├── server.py                 # MCP entry point (stdio / HTTP SSE)
│   ├── project_store.py          # Data access abstraction
│   └── tools/
│       ├── inventory.py          # P0: metadata from manifest
│       ├── query.py              # P1: direct .gdb access
│       ├── izuchennost.py        # P1: survey coverage search
│       ├── attachments.py        # P1: attachments (documents, photos)
│       ├── viz_utils.py          # Shared visualization utilities
│       ├── viz_plot_layer.py     # Static map of a single layer
│       ├── viz_plot_overlay.py   # Static multi-layer composite map
│       ├── viz_histogram.py      # Histograms and distributions
│       ├── viz_interactive.py    # Interactive HTML map (Folium)
│       ├── datacube.py           # Data Cube: ML artifacts from MinIO
│       ├── kg_query.py           # KG: NL queries to Neo4j
│       └── work_type_lookup.py   # KG: geological work type reference
├── api_server/
│   └── server.py                 # OpenAPI Tool Server (FastAPI) for Open WebUI
├── rag/
│   ├── kg_client.py              # Neo4j client (merge_node, merge_rel, execute)
│   ├── kg_builder.py             # Build KG from manifest + PDF cards
│   ├── kg_schema.py              # Node and edge schema
│   ├── nl_to_cypher.py           # NL → Cypher via LLM
│   ├── pdf_parser.py             # PDF survey card parser (Vision LLM / regex)
│   ├── tile_builder.py           # SpatialTile nodes for large layers
│   └── pdf_spec.json             # Work type code reference (Rosgeolfond 1995)
├── static/datacube/
│   ├── index.html                # Data Cube Viewer
│   ├── viewer.js                 # V-block logic
│   └── description.md            # V-block descriptions and data sources
└── ingestion/
    ├── pipeline.py               # Project upload orchestration (7 steps + KG indexing)
    ├── parser_aprx.py            # .aprx parser
    ├── parser_gdb.py             # .gdb parser
    ├── manifest_builder.py       # manifest.json builder
    └── quality.py                # Data quality assessment
```

### Tool Levels (P0 / P1 / Viz)

| Level | Data Source | Speed | Purpose |
|-------|-------------|-------|---------|
| **P0** (Inventory) | `manifest.json` | < 100 ms | Find and describe layers without reading .gdb |
| **P1** (Query) | `.gdb` directly via Fiona/GeoPandas | 1–30 s | Attribute queries, live statistics |
| **Viz** | `.gdb` + manifest | 2–60 s | Generate maps and charts |
| **DataCube** | ML artifacts in MinIO (`MINIO_CUBE_BUCKET`) | < 1 s | Block scores, SHAP, feature importance |

---

## Transport and Server Configuration

**Primary mode (production):** `api_server/server.py` — FastAPI OpenAPI server. Open WebUI reads `/openapi.json` and turns each endpoint into an LLM tool. Connect at `http://localhost:10002/openapi.json`.

**Alternative mode (local/stdio):** `mcp_server/server.py` — FastMCP instance for direct launch via `run_agent.py` / pydantic-ai.

**Shared state** is stored in `_state = {"current_project_id": None}`. Calling `get_project_summary(project_id=X)` sets `current_project_id` to `X`, so subsequent tools don't need the project specified again.

---

## Tool Factory: the make_tools Pattern

Each module exports a `make_tools(store, state) -> list[Callable]` function. Closures capturing `store` and `state` are created inside:

```python
def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    def plot_layer(layer_id: str, ...) -> str:
        pid = state.get("current_project_id") or project_id
        gdb = store.get_gdb_path(pid)
        ...
    return [plot_layer]
```

FastMCP sees ordinary functions with no `store`/`state` in their signature — dependencies are injected via closure.

---

## ProjectStore — Storage Abstraction

`project_store.py` provides a unified read interface:

| Method | Returns |
|--------|---------|
| `list_projects()` | List of `ProjectSummary` from `_index.json` |
| `get_manifest(project_id)` | Full project `manifest.json` |
| `get_layer_entry(manifest, layer_id)` | Layer entry from manifest |
| `get_gdb_path(project_id)` | Path to the `.gdb` file |
| `get_layer_profile(project_id, layer_id)` | Detailed layer profile from `layer_profiles/` |
| `resolve_layer_name(project_id, query)` | Fuzzy layer name matching |

### Layer Name Resolution (5 levels, priority descending)

1. Exact match on `dataset_name` (case-insensitive)
2. Exact match on `display_name`
3. Exact match on any alias
4. Partial match on `display_name` (all query tokens are in the name)
5. Partial match on aliases

---

## Feature Extraction at Ingestion

### Data Sources

When adding a project (`ingestion/pipeline.py`), data is extracted from two sources:

**`.aprx` (ArcGIS Pro Project):**
- `display_name` of each layer (name shown in map legend)
- Group membership (`group`)
- `label_expression` (field used for feature labels)
- `display_field` (field for popups)
- Layer order, visibility

**`.gdb` (File Geodatabase):**
- Geometry type (`geometry_type`): Point, LineString, Polygon, and Multi- variants
- Feature count (`feature_count`)
- CRS (EPSG code, WKT)
- Extent in native CRS and WGS84
- Full field schema with data types
- Field statistics (for layers with ≤ 10,000 features):
  - numeric: min, max, mean, std, nulls
  - categorical: unique_count, top_values (20 values)
- Presence of attachment tables (`*__ATTACH`)

### What Gets Written to manifest.json

```json
{
  "version": "1.0",
  "project": { "id", "name", "source_files", "map": { "primary_crs", "extent_wgs84" } },
  "layers": [
    {
      "layer_id": "gms_r",
      "display_name": "Delta G field (mGal)",
      "display_name_source": "aprx | gdb_only | inferred",
      "group": "Gravimetry R-42",
      "feature_dataset": null,
      "geometry_type": "Point",
      "feature_count": 102216,
      "crs_epsg": 7683,
      "extent_wgs84": [minx, miny, maxx, maxy],
      "units": "mGal",
      "needs_review": false,
      "fields": [ { "name", "dtype", "alias", "nulls", "min", "max", "mean" } ],
      "default_color_field": "Value",
      "aprx_label_expression": "$feature.ID",
      "attachments": { "table": "gms_r__ATTACH", "total": 0 }
    }
  ],
  "groups": { "Gravimetry R-42": { "layers": ["gms_r", "fhg_gr"] } },
  "aliases": { "gms_r": ["gravimetry", "gravity", "delta g field", "mgal"] },
  "quality": { "layers_total", "metadata_completeness", "warnings": [] },
  "mapping_quality": { "mapped_from_aprx", "mapped_from_dict", "needs_review" }
}
```

**Auto-generated aliases:** tokens from `display_name`, `dataset_name` variants (with `_` replaced by space and without), Cyrillic transliteration, semantic keywords by unit of measure (mGal → "gravity", "gravimetry", "delta g").

**`needs_review: true`** — layer found only in `.gdb`, absent from `.aprx`; name may be unreadable.

---

## P0 Tools — Inventory

All tools read only `manifest.json`, with no `.gdb` access.

### `list_projects()`
Returns a JSON list of all projects: `id`, `name`, `layers_count`, `has_attachments`, `created_at`. First step in any agent session.

---

### `get_project_summary(project_id)`
Comprehensive project overview. **Side effect:** sets `current_project_id` in shared state.

Returns:
- `layers_total`, `layers_non_empty`
- `mapping_coverage` (% of layers with readable names)
- `groups` — dict `{group_name: layer_count}`
- `has_attachments`, `attachments_count`
- `crs`, `has_3d_layers`, `metadata_completeness`
- `warnings` — list of quality issues

---

### `list_layers(group, include_needs_review, project_id, output_format)`

List of project layers with optional group filtering.

**Output modes:**
- `"compact"` (default) — grouped plain-text, one line per layer. Saves agent context window (~80% compared to JSON for 50–80 layers):
  ```
  project=lekyn  layers=24
  hint: describe_layer(layer=...) for details

  [Gravimetry R-42]
    gms_r    Delta G field (mGal)               [Point]
    fhg_gr   Full horizontal gradient delta G    [Point]  ⚠

  [no group]
    lin      Lineaments from gravimetry          [MultiLineString]
  ```
  The `⚠` icon marks layers with `needs_review`.
- `"json"` — full JSON structure.

---

### `describe_layer(layer, project_id)`
Full description of a single layer. The `layer` argument accepts fuzzy names (display_name, layer_id, or alias).

Returns:
- Identifiers, group, `feature_dataset`, geometry type, feature count
- `crs_epsg`, `extent_wgs84`
- `units`, `label_expression`
- `fields[]` with value ranges, top-frequency values, null counts
- `warning` if `needs_review`
- `attachments` if present

---

## P1 Tools — Direct .gdb Access

Read data via GeoPandas/Fiona. Slower than P0; used when the manifest is insufficient.

### `query_features(layer, filters, limit, fields, project_id)`
Feature selection with filtering.

**Filter format** (`filters` — JSON string):
```json
{"Value": ">=5.0", "Type": "borehole"}
```
Supported operators: `>=`, `<=`, `>`, `<`, exact numeric match, substring in string fields.

Returns feature attributes (no geometry), `total_after_filter`, `returned`.

---

### `summarize_layer(layer, project_id)`
Fresh statistics from `.gdb`. Used when `describe_layer` has no stats (layers > 10,000 features).

Returns `fields_stats[]`:
- numeric fields: type, min, max, mean, std, nulls
- categorical: type, unique_count, top_values (20 values)

---

### `search_izuchennost(query, year_from, year_to, work_type, scale, limit, project_id)`
Search through survey coverage layers (previously conducted work in the area).

Auto-detection of coverage layers: names containing "izuch", "изученн", "survey", "работ", "opmar".

Fuzzy field matching:
- work type: `vid_iz`, `type`, `vid`
- years: `god_nach` / `god_end`
- scale: `scale`, `masshtab`
- report name: `name_otch`, `name`, `otchet`

---

### `list_attachments(layer, project_id)`
List of project file attachments (documents, photos, reports). Searches `*__ATTACH` tables. Returns filename, content-type, size, feature association.

---

### `extract_attachment(table, index, output_dir, project_id)`
Extract a binary attachment to disk. Returns the path to the saved file.

---

## Data Preprocessing (viz_utils.py)

All visualization tools use a shared data preparation pipeline.

### `load_and_reproject(gdb_path, layer_id, target_epsg=4326)`
Loads a layer from `.gdb` via `geopandas.read_file()` and reprojects to target CRS (default WGS84 / EPSG:4326). Source CRS for Russian geological data is typically EPSG:7683 (GSK-2011).

### `prepare_for_plot(gdf, max_features=50_000)`
Downsampling for rendering performance:
- **Points:** random sample (`random_state=42` for reproducibility)
- **Lines and polygons:** geometry simplification (`tolerance=0.001`)

Returns `(gdf, was_downsampled: bool)`.

### `clip_to_view(gdf, bounds)`
Spatial filter by bounding box. Keeps only features intersecting the rectangle.

### `clip_quantiles(series, low=0.02, high=0.98)`
Clip statistical outliers for color scale. Returns `(vmin, vmax)` for matplotlib normalization.

### `field_stats(series)`
Field statistics: numeric — min/max/mean/median/std/nulls; string — unique_count/top_values/nulls.

### `auto_colormap(field_name, units, display_name)`
Semantic colormap selection:
- mGal (gravimetry) → `"RdYlBu_r"`
- nT (magnetometry) → `"RdBu_r"`
- elevation / relief → `"terrain"`
- gradient → `"magma"`
- default → `"viridis"`

Unit extraction: regex `\(([^)]+)\)` from `display_name`, then lowercase comparison.

### License Boundary

```python
get_license_boundary(project_id, store) -> GeoDataFrame | None
```
Auto-detection: layer whose name contains `"лиценз"`, `"слх"`, or `"licen"`.

```python
get_license_view_bounds(lic_gdf, margin=0.10) -> tuple | None
```
Returns `(minx, miny, maxx, maxy)` with 10% margin on each side. Used as the primary map extent — data is clipped to this area.

```python
draw_license_boundary(ax, lic_gdf)
```
Draws the boundary as a red dashed line (`linestyle="--"`, `color="red"`, `zorder=10`).

### Semantic Layer Styles

```python
get_semantic_style(layer_id, display_name, feature_dataset=None) -> dict | None
```

Priority rules table (`_SEMANTIC_STYLE_RULES`) with 22 entries. Rule: `(name_patterns, feature_dataset_patterns, style)`. First match wins. Search is done on the concatenation `f"{layer_id} {display_name}".lower()`.

**Rule categories:**

| Category | Patterns | Color |
|----------|----------|-------|
| Rivers | river, реки | `#4488FF` linewidth 0.8 |
| Lakes | lake, озёр | `#87CEEB` alpha 0.5 |
| Roads | road, дорог | `#888888` linewidth 0.5 |
| Settlements | town, насел, город | `#8B4513` marker |
| Relief (contours) | relief, горизонт, contour | `#A0785A` linewidth 0.4 |
| Frame / grid | rama, ramka, frame | `#AAAAAA` alpha 0.5 |
| Admin boundary | obl_p, border, boundary | `#666666` linestyle `--` |
| Sheet grid | gridsheet, grid | `#CCCCCC` alpha 0.3 |
| Geophys. isolines | izol, изол, n_pole | `#BBBBBB` linewidth 0.3 |
| Lineaments | lin, lineament | `#00BB44` linewidth 1.0 |
| Positive extrema | extr_pol, положит | `#CC0000` marker `^` |
| Negative extrema | extr_otr, отрицат | `#0055CC` marker `v` |
| Tectonics / faults | tect, fault, разрывн | `#1A1A1A` linewidth 1.2 |
| Ore points | drud, ore, руд | `#FFD700` marker `D` |
| Geochemical halos | вторичн, ореол | `#FFB347` alpha 0.5 |
| Geology (polygons) | geol, basea, mrana | `#B8E8A0` edgecolor `#4A7A30` |
| Profiles | профил, profile | `#FF8C00` linewidth 0.7 |
| Test pits | шурф | `#8B4513` marker `s` |
| Boreholes | скважин, well | `#FFD700` marker `o` edgecolor `#333333` |
| Trenches | канав, trench | `#8B4513` linewidth 1.0 |
| Survey coverage | изучен, opmar, survey | `#90EE90` alpha 0.35 |

If no match — `DEFAULT_STYLES` by geometry type is used.

---

## Visualization Tools

### `plot_layer` — static single-layer map

```python
plot_layer(
    layer_id: str,
    project_id: str | None = None,
    color_field: str | None = None,   # field for colorization
    style: str = "auto",              # "auto"|"scatter"|"lines"|"polygons"
    colormap: str = "auto",           # "auto" or matplotlib colormap name
    show_license: bool = True,
    bbox_wgs84: str | None = None,    # "minx,miny,maxx,maxy"
    title: str | None = None,
    output_format: str = "png",       # "png" | "svg"
) -> str
```

**Color field selection logic:**
1. Explicit `color_field` parameter (fuzzy column search)
2. `default_color_field` from manifest entry
3. For geophysical layers (`units` is set): first numeric field not in `_SKIP_FIELDS`
4. Otherwise — single semantic color without colorbar

**Rendering by data type:**
- **Numeric field:** scatter / colorized lines / filled polygons with colorbar, 2%–98% quantile clipping
- **Categorical field:** tab20 palette, legend (up to 20 values)
- **No field:** semantic color from `get_semantic_style()`

**Map extent:** if `show_license=True`, view bounds are set from `get_license_view_bounds()`.

**Downsampling:** if > 50,000 features — random sample with a warning in the response.

Returns JSON: `file`, `url`, `markdown`, `layer`, `display_name`, `feature_count`, `geometry_type`, `color_field`, `colormap`, `style`, `field_stats`, `warning`.

---

### `plot_overlay` — composite multi-layer map

```python
plot_overlay(
    layers: str,            # JSON array of layer spec objects
    project_id: str | None = None,
    show_license: bool = True,
    show_legend: bool = True,
    title: str | None = None,
    output_format: str = "png",
) -> str
```

**Layer spec format:**
```json
[
  {"layer_id": "relief",        "alpha": 0.3, "linewidth": 0.2},
  {"layer_id": "river",         "label": "Rivers"},
  {"layer_id": "Boreholes_GSK", "color": "red", "marker": "o", "markersize": 12, "label": "Boreholes"}
]
```

**Style priority:** semantic style → `DEFAULT_STYLES` by geometry type → spec overrides.

**Rendering order:**
1. Load license boundary, compute `view_bounds`
2. For each layer: load → reproject → clip to `view_bounds` → render
3. Draw license boundary on top (`zorder=10`)
4. Add legend

Returns JSON: `file`, `url`, `markdown`, `layers_rendered`, `layers_requested`.

---

### `plot_histogram` — statistical distribution

```python
plot_histogram(
    layer_id: str,
    field: str,
    project_id: str | None = None,
    plot_type: str = "auto",     # "auto"|"histogram"|"bar"|"bar_top20"|"boxplot"
    group_by: str | None = None, # grouping field for boxplot
    bins: int = 50,
    title: str | None = None,
    output_format: str = "png",
) -> str
```

**Auto plot type selection:**
- numeric field, < 15 unique values → `"bar"`
- numeric field, ≥ 15 → `"histogram"`
- string field, ≤ 30 unique → `"bar"`
- string field, > 30 → `"bar_top20"`

**Plot types:**
- `"histogram"` — histogram with mean and median lines
- `"bar"` / `"bar_top20"` — horizontal bar chart of top values
- `"boxplot"` — box and whisker plots grouped by `group_by`

---

### `plot_interactive` — interactive HTML map (Folium)

```python
plot_interactive(
    layers: str,                       # JSON array of layer IDs
    project_id: str | None = None,
    tooltip_fields: str | None = None, # JSON {layer_id: [fields]}
    center: str | None = None,         # "[lat, lon]"
    zoom: int = 10,
    max_features_per_layer: int = 500,
    style_overrides: str | None = None # JSON {layer_id: {color, weight, ...}}
) -> str
```

**Features:**
- `LayerControl` — toggle layer visibility
- Tooltips: fields selected automatically via `auto_tooltip_fields()` (priority: display_field from `.aprx` → name-type fields → first non-numeric fields)
- Map center and zoom from license boundary (`fit_bounds`)
- Layer truncation: if features > `max_features_per_layer`, uses first N with a warning

Returns JSON: `file`, `url`, `link` (markdown link), `layers_rendered`, `map_center`, `zoom`, `warnings[]`.

---

## Artifact Storage — MinIO

All visualization tools save files locally and upload to MinIO (S3-compatible object storage).

### Configuration

```python
MINIO_ENDPOINT    = "ip:9000"          # internal address (docker network)
MINIO_PUBLIC_HOST = "localhost:9000"   # public address for URLs
MINIO_ACCESS_KEY  = "minio"
MINIO_SECRET_KEY  = "password"
MINIO_BUCKET      = "gis-viz"          # bucket for visualizations (PNG, HTML)
MINIO_CUBE_BUCKET = "gisportal"        # bucket for Data Cube ML artifacts
```

All parameters are overridden via environment variables.

### Artifact Upload Pipeline

```
plot_layer() / plot_overlay() / ...
    │
    ├─ save_figure(fig, pid, name, fmt)
    │    └── projects/{project_id}/viz/{name}.{fmt}  (local file)
    │
    └─ upload_to_minio(local_path, project_id)
         ├── _get_minio()  — lazy singleton client
         ├── _ensure_bucket()  — create bucket + public policy if not exists
         ├── put_object("gis-viz/{project_id}/{filename}", ...)
         └── return "http://{MINIO_PUBLIC_HOST}/gis-viz/{project_id}/{filename}"
```

**Bucket policy:** public read-only (anonymous GET) is set on creation.

**Degradation when unavailable:** if MinIO is unreachable, the tool returns `url: null` and `markdown: null`, and puts the local path in `file`. The agent notifies the user to download the file locally.

### What Tools Return

```json
{
  "file":     "projects/lekyn/viz/gms_r_1708772400.png",
  "url":      "http://localhost:9000/gis-viz/lekyn/gms_r_1708772400.png",
  "markdown": "![Delta G field (mGal)](http://localhost:9000/gis-viz/lekyn/gms_r_1708772400.png)"
}
```

The `markdown` field lets the agent embed the image directly in its response (supported by Open WebUI, Claude Desktop, and other MCP clients with Markdown rendering).

---

## GIS Data Hub — Web Portal (Ingestion API)

`ingestion/app.py` — FastAPI service (`gis-loader`) available at `http://localhost:10003`.

- **Web portal:** `/ui/` — Vue 3 SPA (`static/index.html` + `app.js`). Project management: upload, view manifest, run Data Cube, delete.
- **Auth:** Basic Auth + session cookie (`gis_session`, TTL 8 h). All write endpoints and artifact files are protected.
- **Project upload:** `POST /api/upload` — accepts `.zip` with GDB and optional `.aprx`, runs ingestion pipeline synchronously.
- **Data Cube proxy:** `POST /api/datacube/jobs`, `GET /api/datacube/jobs/{id}` — proxy requests to the `data-cube` service.
- **Artifacts:** `GET /api/projects/{id}/datacube/files/{path}` — serves pipeline result files with Basic Auth / session cookie / `?_auth=` query param. HTML files receive injected `dashboard-override.css` and `lightbox.js`.
- **Cache-Control:** all `/ui/*` routes are served with `no-cache, must-revalidate` — a regular F5 is enough to pick up updates.

---

## REST API (Open WebUI)

`api_server/server.py` wraps all MCP tools as FastAPI endpoints for integration with Open WebUI or direct HTTP calls.

- **Swagger UI:** `http://localhost:10002/docs`
- **OpenAPI JSON:** `http://localhost:10002/openapi.json`

All endpoints accept the same parameters as MCP tools (POST with JSON body) and return the same JSON responses. Includes Data Cube endpoints (`/datacube_overview`, `/datacube_block_scores`, `/datacube_block_detail`).

---

## Data Cube — ML Prospectivity Artifacts

Data Cube is the result of running the ML pipeline (`run_pipeline`) of the `data-cube` service. Artifacts are uploaded to MinIO (`MINIO_CUBE_BUCKET/{project_id}/`) and accessible through three tools.

### Artifact Structure

```
{project_id}/
├── blocks.csv                                    # block grid: coordinates, size
├── scores.csv                                    # prospectivity score (0–1) per block
├── features.csv                                  # feature values per block
├── labels.csv                                    # labels: label_y, dist_nearest_ore_m, weight_w
├── eval_report.json                              # pr_auc, capture_efficiency (x*, x_star, curve)
├── model_meta.json                               # model_type, feature_names, cv
├── run_meta.json                                 # cube_spec, label_spec, train_spec, layer_mapping
└── interpretability/
    ├── global_importance_features.csv            # feature, importance, group
    ├── global_importance_groups.csv              # group, mean, std
    ├── dominant_driver_group.csv                 # block_id, dominant_driver_group
    ├── ale_1d.csv                                # feature, bin_center, ale
    ├── shap_values.csv                           # WIDE: block_id + columns per feature
    ├── shap_geo_unit_summary.csv                 # geo_unit, feature, mean_shap
    └── interpret_global.json                     # importance summary + grouped
```

### Tools

#### `datacube_overview(project_id?)`
First call. Reads `eval_report.json`, `model_meta.json`, `scores.csv`, `global_importance_features.csv`, `dominant_driver_group.csv`.

Returns:
- `artifacts_present` — list of found files
- model metrics: `model_type`, `pr_auc`, `cv_mean_pr_auc`
- `capture_efficiency`: `x_star`, `score_threshold_at_x_star`
- `score_distribution`: n_blocks, min/max/mean, high_confidence_count
- `top3_features` — top-3 features by importance
- `dominant_driver_groups` — dict `{group: block_count}`
- `hint` — next call suggestion

---

#### `datacube_block_scores(project_id?, top_n=20, min_score?)`
Reads `scores.csv`, `blocks.csv`, `dominant_driver_group.csv`.

Returns sorted block list: `rank`, `block_id`, `score`, `lon`, `lat`, `dominant_driver_group`. `top_n` clamped to [1, 200].

---

#### `datacube_block_detail(block_id, project_id?)`
Reads `scores.csv`, `blocks.csv`, `features.csv`, `shap_values.csv`, `dominant_driver_group.csv`.

Returns full block profile:
- `location`: lon, lat, x_m, y_m, row, col, cell_size_m
- `score` + `rank_in_dataset`
- `features`: all feature values
- `shap_values`: list of `{feature, shap}` sorted by `|shap|`
- `dominant_driver`, `dominant_driver_group`

---

### Docker service data-cube

The `data-cube` service in `docker-compose.yml` runs the ML pipeline FastAPI server (`Data_cube/api/server.py`). Accepts `POST /jobs` from `gis-loader` and runs the full experiment pipeline.

```yaml
data-cube:
  build:
    context: .
    dockerfile: Dockerfile.datacube
    args:
      GITHUB_TOKEN: ${GITHUB_TOKEN}
      CACHE_BUST: ${CACHE_BUST:-1}   # invalidates git clone layer
  env_file: .env
  environment:
    - PROJECTS_DIR=/app/projects
```

**Docker cache invalidation** (updating `Data_cube` code from repo without `--no-cache`):
```bash
CACHE_BUST=$(date +%s) docker compose up --build data-cube -d
```

**Hot-swap server.py without image rebuild** (via Makefile):
```bash
make reload-cube
# docker cp Data_cube/api/server.py data-cube-server:/app/data_cube/api/server.py
# docker restart data-cube-server
```

**Stage-by-stage progress tracking** is implemented in `Data_cube/api/server.py` via `_tracked(fn, stage_name)` wrappers around each pipeline function. When the wrapper is called, `_set_stage(job_id, stage_name)` updates the `stage` field in `_jobs`. Clients read it via polling `GET /jobs/{job_id}`.

Stage order: `grid` → `features` → `qa` → `labels` → `training` → `evaluation` → `visualization` → `upload`.

---

## Knowledge Graph (Neo4j)

When ingesting a project, `pipeline.py` automatically builds a knowledge graph in Neo4j (step 6/7). KG is non-blocking: if Neo4j is unavailable, the pipeline completes successfully and KG tools degrade to empty responses.

### Graph Schema

**Nodes:** `Project`, `Group`, `Layer`, `Field`, `Attachment`, `InvestigationCard`, `Mineral`, `Organization`, `WorkMethod`, `SpatialTile`, `DatacubeBlock`

**Edges:**
```
(Project)-[:HAS_LAYER]          ->(Layer)
(Project)-[:HAS_GROUP]          ->(Group)
(Project)-[:HAS_BLOCK]          ->(DatacubeBlock)
(Layer)-[:HAS_FIELD]            ->(Field)
(Layer)-[:HAS_ATTACHMENT]       ->(Attachment)
(Attachment)-[:IS_CARD]         ->(InvestigationCard)
(InvestigationCard)-[:TARGETS]          ->(Mineral)
(InvestigationCard)-[:CONDUCTED_BY]     ->(Organization)
(InvestigationCard)-[:USES_METHOD]      ->(WorkMethod)
(InvestigationCard)-[:SPATIALLY_COVERS]->(Layer)
```

### Survey Card PDF Parser

`rag/pdf_parser.py` extracts structured data from PDF attachments (Rosgeolfond survey cards, fields 1–28). Two modes:

- **Vision LLM** (if `KG_LLM_MODEL` is set): fitz renders pages to PNG → base64 → vLLM (Pixtral/Mistral).
- **Regex fallback** (`KG_LLM_MODEL` empty): fitz extracts text → regex on card fields.

### NL → Cypher

`rag/nl_to_cypher.py` converts natural language questions to Cypher queries via LLM. The system prompt contains the full schema, canonical patterns, and 12 generation rules (including automatic `LIMIT 50` without a filter).

### KG Tools

#### `geo_context_query(query, project_id?)`
Executes an NL query against the Knowledge Graph. Converts question → Cypher → executes in Neo4j → returns results. Useful for finding relationships between entities (minerals, organizations, work methods, survey cards) that are not directly accessible through manifest or .gdb.

---

#### `lookup_work_types(codes)`
Reference for geological work type codes (field 8 of the survey card, Rosgeolfond 1995 classifier). Accepts a list of string codes, returns the decoded descriptions.

---

## Tool Summary

| Tool | Level | Source | Purpose |
|------|-------|--------|---------|
| `list_projects` | P0 | manifest | List all projects |
| `get_project_summary` | P0 | manifest | Project overview, set context |
| `list_layers` | P0 | manifest | Layer list (compact or JSON) |
| `describe_layer` | P0 | manifest | Layer details: fields, stats, extent |
| `query_features` | P1 | .gdb | Feature selection with filters |
| `summarize_layer` | P1 | .gdb | Fresh field statistics |
| `search_izuchennost` | P1 | .gdb | Search survey coverage layers |
| `list_attachments` | P1 | .gdb | List attachments (documents, photos) |
| `extract_attachment` | P1 | .gdb | Extract attachment to disk |
| `plot_layer` | Viz | .gdb + manifest | Static single-layer map (PNG/SVG) |
| `plot_overlay` | Viz | .gdb + manifest | Composite multi-layer map |
| `plot_histogram` | Viz | .gdb + manifest | Histogram / bar chart |
| `plot_interactive` | Viz | .gdb + manifest | Interactive HTML map (Folium) |
| `datacube_overview` | DataCube | MinIO (MINIO_CUBE_BUCKET) | Model metrics, top features, score distribution |
| `datacube_block_scores` | DataCube | MinIO | Ranked block list by score |
| `datacube_block_detail` | DataCube | MinIO | Full block profile: features, SHAP, driver |
| `geo_context_query` | KG | Neo4j | NL query to Knowledge Graph (minerals, orgs, methods) |
| `lookup_work_types` | KG | Neo4j | Decode geological work type codes |
