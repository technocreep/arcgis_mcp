"""Tool: plot_interactive — интерактивная Plotly Mapbox карта.

Мультислойная карта с поддержкой:
- Спутниковой/OSM/CartoDB подложки без токена
- Физических полей (density → PNG-оверлей с griddata, heatmap → Densitymapbox)
- Базовых условных знаков для точечных объектов
- Линий (в т.ч. MultiLineString) и полигонов
- GK-координат в hover (автодетект зоны по долготе центроида)
- Переключения слоёв через стандартную легенду Plotly
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from pathlib import Path
from typing import Callable

import geopandas as gpd
import numpy as np
import plotly.graph_objects as go
from pyproj import Transformer

from arcgis_mcp.config import PROJECTS_DIR
from arcgis_mcp.mcp_server.tools.viz_utils import (
    auto_colormap,
    auto_tooltip_fields,
    clip_quantiles,
    get_license_boundary,
    get_semantic_style,
    load_and_reproject,
    prepare_for_plot,
    upload_to_minio,
)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

_BASEMAPS: dict[str, dict] = {
    "satellite": {
        "style": "white-bg",
        "layers": [{
            "sourcetype": "raster",
            "source": [
                "https://server.arcgisonline.com/ArcGIS/rest/services/"
                "World_Imagery/MapServer/tile/{z}/{y}/{x}"
            ],
            "below": "traces",
        }],
    },
    "osm":   {"style": "open-street-map"},
    "carto": {"style": "carto-positron"},
    "dark":  {"style": "carto-darkmatter"},
}

# (подстроки для поиска в layer_id + display_name) → (plotly symbol, размер)
_SYMBOL_RULES: list[tuple[tuple[str, ...], str, int]] = [
    (("скваж", "well", "borehole", "скв"),    "diamond",        10),
    (("канав", "trench", "канава"),            "square",          8),
    (("шурф",),                                "square",          8),
    (("проб", "sample"),                       "circle-stroked",  7),
    (("руд", "ore", "mineral", "рудопр"),      "star",           10),
    (("наблюд", "obs", "punkt", "пункт"),      "circle",          6),
    (("точк",),                                "circle",          6),
]
_DEFAULT_SYMBOL = ("circle", 8)

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _point_symbol(layer_id: str, display_name: str) -> tuple[str, int]:
    name = (layer_id + " " + display_name).lower()
    for substrings, sym, sz in _SYMBOL_RULES:
        if any(s in name for s in substrings):
            return sym, sz
    return _DEFAULT_SYMBOL


def _to_gk(lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Конвертировать WGS84 → GK по автодетектированной зоне. Возвращает (N, 2)."""
    if len(lons) == 0:
        return np.zeros((0, 2))
    lon_center = float(np.nanmean(lons))
    zone = max(1, min(32, int((lon_center + 3) / 6) + 1))
    epsg_gk = 28400 + zone
    try:
        tr = Transformer.from_crs(4326, epsg_gk, always_xy=True)
        xgk, ygk = tr.transform(lons, lats)
        return np.stack([xgk, ygk], axis=1)
    except Exception:
        return np.zeros((len(lons), 2))


def _auto_zoom(minlon: float, minlat: float, maxlon: float, maxlat: float) -> int:
    span = max(maxlon - minlon, maxlat - minlat)
    for threshold, z in [(20, 4), (10, 5), (5, 6), (2, 8), (0.5, 10), (0.1, 12)]:
        if span > threshold:
            return z
    return 13


def _hex_to_rgba(color: str, alpha: float = 0.4) -> str:
    """Hex → CSS rgba()."""
    if not (color.startswith("#") and len(color) in (7, 9)):
        return f"rgba(100,100,200,{alpha})"
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _make_hover(row, fields: list[str], gk: tuple[float, float] | None) -> str:
    parts = []
    if gk is not None:
        parts.append(f"X: {gk[0]:.0f} м | Y: {gk[1]:.0f} м")
    for f in fields:
        try:
            val = row[f]
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                parts.append(f"<b>{f}:</b> {val}")
        except (KeyError, TypeError):
            pass
    return "<br>".join(parts) if parts else ""


def _explode_line(geom) -> tuple[list, list]:
    """MultiLineString/LineString → lat[], lon[] с None-разделителями."""
    lats, lons = [], []
    parts = list(geom.geoms) if hasattr(geom, "geoms") else [geom]
    for part in parts:
        xs, ys = part.xy
        lons.extend(list(xs))
        lats.extend(list(ys))
        lons.append(None)
        lats.append(None)
    return lats, lons


def _explode_polygon(geom) -> tuple[list, list]:
    """MultiPolygon/Polygon → lat[], lon[] с None-разделителями (только внешний контур)."""
    lats, lons = [], []
    polys = list(geom.geoms) if hasattr(geom, "geoms") else [geom]
    for poly in polys:
        xs, ys = poly.exterior.xy
        lons.extend(list(xs))
        lats.extend(list(ys))
        lons.append(None)
        lats.append(None)
    return lats, lons


# ---------------------------------------------------------------------------
# Density PNG оверлей
# ---------------------------------------------------------------------------

def _render_density_png(
    gdf: gpd.GeoDataFrame,
    color_field: str,
    colormap_name: str,
    pid: str,
    layer_id: str,
) -> tuple[dict | None, tuple[float, float] | None]:
    """griddata-интерполяция → RGBA PNG → конфиг Mapbox image layer.

    Результат кешируется в projects/{pid}/viz/cache/density_{hash}.png.
    """
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
    from scipy.interpolate import griddata

    x = gdf.geometry.x.values
    y = gdf.geometry.y.values
    try:
        z = gdf[color_field].values.astype(float)
    except Exception:
        return None, None

    mask = np.isfinite(z)
    if mask.sum() < 9:
        return None, None
    x, y, z = x[mask], y[mask], z[mask]

    vmin = float(np.nanpercentile(z, 2))
    vmax = float(np.nanpercentile(z, 98))
    if vmin == vmax:
        return None, None

    # Ключ кеша по экстенту и полю
    cache_key = hashlib.md5(
        f"{layer_id}_{color_field}_{x.min():.5f}_{x.max():.5f}"
        f"_{y.min():.5f}_{y.max():.5f}_{vmin:.3f}_{vmax:.3f}".encode()
    ).hexdigest()[:12]
    cache_dir = Path(PROJECTS_DIR) / pid / "viz" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"density_{cache_key}.png"

    if not cache_path.exists():
        grid_n = 250
        gx, gy = np.meshgrid(
            np.linspace(x.min(), x.max(), grid_n),
            np.linspace(y.min(), y.max(), grid_n),
        )
        grid_z = griddata((x, y), z, (gx, gy), method="linear")

        cmap = plt.cm.get_cmap(colormap_name)
        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        rgba = cmap(norm(grid_z))           # (H, W, 4), float [0,1]
        rgba[np.isnan(grid_z), 3] = 0.0    # NaN → прозрачный
        rgba[~np.isnan(grid_z), 3] = 0.82  # данные → полупрозрачные

        # plt.imsave: origin='upper' = y=0 сверху, соответствует lat.max() вверху
        plt.imsave(str(cache_path), np.flipud(rgba))

    # Загрузить в MinIO или закодировать в base64
    url = upload_to_minio(str(cache_path), pid)
    if url is None:
        with open(cache_path, "rb") as f:
            url = "data:image/png;base64," + base64.b64encode(f.read()).decode()

    # Координаты углов: [NW, NE, SE, SW]
    layer_cfg = {
        "sourcetype": "image",
        "source": url,
        "coordinates": [
            [float(x.min()), float(y.max())],
            [float(x.max()), float(y.max())],
            [float(x.max()), float(y.min())],
            [float(x.min()), float(y.min())],
        ],
        "below": "traces",
    }
    return layer_cfg, (vmin, vmax)


def _colorbar_trace(
    vmin: float,
    vmax: float,
    colorscale: str,
    label: str,
    lat: float,
    lon: float,
) -> go.Scattermapbox:
    """Невидимый trace для отображения colorbar density-оверлея."""
    return go.Scattermapbox(
        lat=[lat], lon=[lon],
        mode="markers",
        marker={
            "color": [vmin, vmax],
            "colorscale": colorscale,
            "showscale": True,
            "colorbar": {"title": {"text": label, "side": "right"}, "thickness": 14, "len": 0.6},
            "size": 1,
            "opacity": 0,
        },
        hoverinfo="skip",
        showlegend=False,
    )


# ---------------------------------------------------------------------------
# make_tools
# ---------------------------------------------------------------------------

def make_tools(store, state: dict) -> list[Callable]:

    def plot_interactive(
        layers: str,
        project_id: str | None = None,
        basemap: str = "satellite",
        zoom: int | None = None,
        title: str | None = None,
    ) -> str:
        """Создать интерактивную HTML-карту (Plotly Mapbox) для нескольких слоёв.

        Лучший инструмент для исследования данных: зум, клик, подписи атрибутов,
        переключение слоёв через легенду, спутниковая подложка.

        Рекомендации по выбору style в layer spec:
        - Не указан / "auto" — автодетект: точки >1000 + числовое поле → density.
        - "density" — физическое поле (гравика, магнитка, радиометрия):
                      griddata-интерполяция → PNG-оверлей с colorbar.
        - "heatmap"  — плотностная тепловая карта точек (go.Densitymapbox).
        - "scatter"  — дискретные точки (скважины, пробы, канавы).
        - "lines"    — принудительно линии.
        - "polygons" — принудительно полигоны.

        Args:
            layers: JSON-массив layer spec. Каждый элемент:
                    {"layer_id": "...", "style": "auto", "color_field": "...",
                     "colormap": "auto", "color": "#RRGGBB"}
                    Пример:
                    '[{"layer_id": "wells"}, {"layer_id": "gravity_pts",
                      "style": "density", "color_field": "dg"}]'
            project_id: ID проекта (необязательно, если задан через get_project_summary).
            basemap: "satellite" (ESRI World Imagery, по умолчанию), "osm", "carto", "dark".
            zoom: Начальный зум Mapbox (None → авто по extent).
            title: Заголовок карты (None → авто из имён слоёв).
        """
        try:
            pid = project_id or state.get("current_project_id")
            if not pid:
                return json.dumps({"error": "project_id обязателен."}, ensure_ascii=False)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            layer_specs: list[dict] = json.loads(layers)
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "layers: невалидный JSON-массив."}, ensure_ascii=False)

        traces: list[go.BaseTraceType] = []
        extra_mapbox_layers: list[dict] = []
        all_bounds: list[np.ndarray] = []
        warnings_out: list[str] = []

        for spec in layer_specs:
            raw_id = spec.get("layer_id", "")
            resolved_id = store.resolve_layer_name(pid, raw_id) or raw_id
            entry = store.get_layer_entry(manifest, resolved_id) or {}
            display_name = entry.get("display_name", resolved_id)

            try:
                gdf = load_and_reproject(gdb_path, resolved_id)
            except Exception as e:
                warnings_out.append(f"'{resolved_id}': ошибка загрузки — {e}")
                continue

            if gdf.empty:
                warnings_out.append(f"'{resolved_id}': слой пустой.")
                continue

            gdf, _ = prepare_for_plot(gdf)
            all_bounds.append(gdf.total_bounds)

            gt = entry.get("geometry_type") or gdf.geometry.geom_type.mode().iloc[0]
            gt_lower = (gt or "").lower()
            feature_count = entry.get("feature_count", len(gdf))

            spec_style = spec.get("style", "auto")
            color_field = spec.get("color_field") or entry.get("default_color_field")
            if color_field and color_field not in gdf.columns:
                color_field = None

            sem = get_semantic_style(resolved_id, display_name, entry.get("feature_dataset")) or {}
            spec_color = spec.get("color") or sem.get("color", "#4488CC")
            colormap_name = spec.get("colormap", "auto")
            if colormap_name == "auto":
                colormap_name = auto_colormap(color_field, entry.get("units"), display_name)

            tooltip_fields = auto_tooltip_fields(gdf, entry)

            # ── Точки ──────────────────────────────────────────────────────
            if "point" in gt_lower:
                is_density = (
                    spec_style == "density"
                    or (
                        spec_style == "auto"
                        and color_field
                        and color_field in gdf.columns
                        and np.issubdtype(gdf[color_field].dtype, np.number)
                        and feature_count > 1000
                    )
                )

                if spec_style == "heatmap":
                    z_vals = None
                    if color_field and color_field in gdf.columns:
                        try:
                            z_vals = gdf[color_field].values.astype(float)
                        except Exception:
                            pass
                    traces.append(go.Densitymapbox(
                        lat=gdf.geometry.y.values,
                        lon=gdf.geometry.x.values,
                        z=z_vals,
                        radius=12,
                        colorscale=colormap_name,
                        showscale=True,
                        colorbar={"title": {"text": color_field or display_name}, "thickness": 14},
                        name=display_name,
                        hovertemplate=(
                            f"<b>{display_name}</b>"
                            + (f"<br>{color_field}: %{{z:.2f}}" if z_vals is not None else "")
                            + "<extra></extra>"
                        ),
                    ))

                elif is_density and color_field:
                    layer_cfg, vrange = _render_density_png(
                        gdf, color_field, colormap_name, pid, resolved_id
                    )
                    if layer_cfg and vrange:
                        extra_mapbox_layers.append(layer_cfg)
                        units = entry.get("units", "")
                        cb_label = f"{color_field}{' (' + units + ')' if units else ''}"
                        traces.append(_colorbar_trace(
                            vrange[0], vrange[1], colormap_name, cb_label,
                            float(gdf.geometry.y.mean()),
                            float(gdf.geometry.x.mean()),
                        ))
                        # Добавить невидимый trace в легенду чтобы слой можно было скрыть
                        traces.append(go.Scattermapbox(
                            lat=[float(gdf.geometry.y.mean())],
                            lon=[float(gdf.geometry.x.mean())],
                            mode="markers",
                            marker={"size": 1, "opacity": 0},
                            name=display_name,
                            hoverinfo="skip",
                            showlegend=True,
                        ))
                    else:
                        warnings_out.append(f"'{display_name}': density-рендеринг не удался.")

                else:
                    # Стандартный scatter
                    sym, sym_size = _point_symbol(resolved_id, display_name)
                    lats_arr = gdf.geometry.y.values
                    lons_arr = gdf.geometry.x.values
                    gk_arr = _to_gk(lons_arr, lats_arr)

                    hover_texts = []
                    for idx, (_, row) in enumerate(gdf.iterrows()):
                        gk = (float(gk_arr[idx, 0]), float(gk_arr[idx, 1]))
                        hover_texts.append(_make_hover(row, tooltip_fields, gk))

                    if color_field and np.issubdtype(gdf[color_field].dtype, np.number):
                        vmin, vmax = clip_quantiles(gdf[color_field])
                        marker_cfg = {
                            "symbol": sym,
                            "size": sym_size,
                            "color": gdf[color_field].values.tolist(),
                            "colorscale": colormap_name,
                            "cmin": vmin,
                            "cmax": vmax,
                            "showscale": True,
                            "colorbar": {
                                "title": {"text": f"{color_field}{' (' + entry.get('units','') + ')' if entry.get('units') else ''}"},
                                "thickness": 14,
                            },
                        }
                    else:
                        marker_cfg = {"symbol": sym, "size": sym_size, "color": spec_color}

                    traces.append(go.Scattermapbox(
                        mode="markers",
                        lat=lats_arr,
                        lon=lons_arr,
                        marker=marker_cfg,
                        name=display_name,
                        text=hover_texts,
                        hovertemplate="<b>" + display_name + "</b><br>%{text}<extra></extra>",
                    ))

            # ── Линии ──────────────────────────────────────────────────────
            elif "line" in gt_lower or "string" in gt_lower:
                line_color = spec_color if spec_color.startswith("#") else "#4488CC"
                linewidth = float(sem.get("linewidth", spec.get("linewidth", 1.5)))

                all_lats: list = []
                all_lons: list = []
                for geom in gdf.geometry:
                    if geom is None or geom.is_empty:
                        continue
                    lats_g, lons_g = _explode_line(geom)
                    all_lats.extend(lats_g)
                    all_lons.extend(lons_g)

                traces.append(go.Scattermapbox(
                    mode="lines",
                    lat=all_lats,
                    lon=all_lons,
                    line={"color": line_color, "width": linewidth},
                    name=display_name,
                    hovertemplate=f"<b>{display_name}</b><extra></extra>",
                ))

            # ── Полигоны ───────────────────────────────────────────────────
            else:
                base_color = spec_color if spec_color.startswith("#") else "#4488CC"
                fill_rgba = _hex_to_rgba(base_color, 0.35)
                edge_color = sem.get("edgecolor", "#555555")

                all_lats = []
                all_lons = []
                for geom in gdf.geometry:
                    if geom is None or geom.is_empty:
                        continue
                    lats_g, lons_g = _explode_polygon(geom)
                    all_lats.extend(lats_g)
                    all_lons.extend(lons_g)

                traces.append(go.Scattermapbox(
                    mode="lines",
                    lat=all_lats,
                    lon=all_lons,
                    fill="toself",
                    fillcolor=fill_rgba,
                    line={"color": edge_color, "width": 1},
                    name=display_name,
                    hovertemplate=f"<b>{display_name}</b><extra></extra>",
                ))

        # ── Контур лицензии ────────────────────────────────────────────────
        lic_gdf = get_license_boundary(pid, store)
        if lic_gdf is not None and not lic_gdf.empty:
            lic_lats: list = []
            lic_lons: list = []
            for geom in lic_gdf.geometry:
                if geom is None or geom.is_empty:
                    continue
                lats_g, lons_g = _explode_polygon(geom)
                lic_lats.extend(lats_g)
                lic_lons.extend(lons_g)
            traces.append(go.Scattermapbox(
                mode="lines",
                lat=lic_lats,
                lon=lic_lons,
                line={"color": "red", "width": 2},
                name="Контур лицензии",
                hovertemplate="<b>Контур лицензии</b><extra></extra>",
            ))
            all_bounds.append(lic_gdf.total_bounds)

        if not traces:
            return json.dumps({"error": "Ни один слой не загружен.", "warnings": warnings_out},
                              ensure_ascii=False)

        # ── Центр и зум ────────────────────────────────────────────────────
        if all_bounds:
            combined = np.array(all_bounds)
            minlon = float(combined[:, 0].min())
            minlat = float(combined[:, 1].min())
            maxlon = float(combined[:, 2].max())
            maxlat = float(combined[:, 3].max())
        else:
            minlon, minlat, maxlon, maxlat = 37.0, 55.0, 38.0, 56.0

        center_lat = (minlat + maxlat) / 2
        center_lon = (minlon + maxlon) / 2
        auto_z = zoom if zoom is not None else _auto_zoom(minlon, minlat, maxlon, maxlat)

        # ── Конфиг подложки ────────────────────────────────────────────────
        bm = _BASEMAPS.get(basemap, _BASEMAPS["satellite"])
        mapbox_cfg: dict = {
            "style": bm["style"],
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": auto_z,
        }
        all_extra = list(bm.get("layers", [])) + extra_mapbox_layers
        if all_extra:
            mapbox_cfg["layers"] = all_extra

        # ── Фигура ─────────────────────────────────────────────────────────
        auto_title = title or ", ".join(
            s.get("layer_id", "") for s in layer_specs[:4]
        )
        fig = go.Figure(data=traces)
        fig.update_layout(
            title={"text": auto_title, "x": 0.5, "font": {"size": 14}},
            mapbox=mapbox_cfg,
            margin={"l": 0, "r": 0, "t": 40, "b": 0},
            legend={
                "bgcolor": "rgba(255,255,255,0.85)",
                "bordercolor": "#cccccc",
                "borderwidth": 1,
                "font": {"size": 11},
            },
        )

        # ── Динамическая масштабная линейка (JS) ───────────────────────────
        scalebar_js = """
(function() {
    var gd = document.querySelector('.plotly-graph-div');
    if (!gd) return;

    // Создаём DOM-элемент линейки поверх карты
    var bar = document.createElement('div');
    bar.id = 'dyn-scale-bar';
    bar.style.cssText = [
        'position:absolute', 'bottom:28px', 'left:14px',
        'display:flex', 'align-items:center', 'gap:5px',
        'background:rgba(0,0,0,0.50)', 'color:#fff',
        'padding:4px 8px', 'border-radius:3px',
        'font:12px/1 Arial,sans-serif', 'pointer-events:none', 'z-index:999'
    ].join(';');
    gd.style.position = 'relative';
    gd.appendChild(bar);

    function niceDistance(meters) {
        var mag = Math.pow(10, Math.floor(Math.log10(meters)));
        var f = meters / mag;
        var nice = f < 1.5 ? mag : f < 3.5 ? 2*mag : f < 7.5 ? 5*mag : 10*mag;
        var label = nice >= 1000 ? (nice/1000).toFixed(0)+'\u00a0км' : nice.toFixed(0)+'\u00a0м';
        return {meters: nice, label: label};
    }

    function update(zoom, lat) {
        // Метров на пиксель при данном зуме и широте
        var mpp = 40075016.686 * Math.cos(lat * Math.PI / 180) / (512 * Math.pow(2, zoom));
        var target = niceDistance(mpp * 100);   // ширина ~100px
        var barPx = Math.round(target.meters / mpp);
        bar.innerHTML =
            '<span style="display:inline-block;width:'+barPx+'px;height:3px;'+
            'background:#fff;border:1px solid #fff;box-sizing:border-box"></span>' +
            '<span>'+target.label+'</span>';
    }

    function getMapboxState() {
        try {
            var mb = gd.layout.mapbox;
            return { zoom: mb.zoom || 8, lat: (mb.center || {}).lat || 60 };
        } catch(e) { return {zoom: 8, lat: 60}; }
    }

    gd.on('plotly_relayout', function(ev) {
        var s = getMapboxState();
        // relayout даёт нам обновлённые значения напрямую
        var z = (ev['mapbox.zoom'] != null) ? ev['mapbox.zoom'] : s.zoom;
        var lat = (ev['mapbox.center.lat'] != null) ? ev['mapbox.center.lat'] : s.lat;
        update(z, lat);
    });

    // Первый рендер — ждём plotly_afterplot
    gd.on('plotly_afterplot', function() {
        var s = getMapboxState();
        update(s.zoom, s.lat);
    });
})();
"""

        # ── Сохранение HTML ────────────────────────────────────────────────
        out_dir = Path(PROJECTS_DIR) / pid / "viz"
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = f"interactive_{int(time.time())}.html"
        out_path = out_dir / fname
        fig.write_html(
            str(out_path),
            include_plotlyjs="cdn",
            full_html=True,
            post_script=scalebar_js,
        )

        url = upload_to_minio(str(out_path), pid)
        result: dict = {
            "file": str(out_path),
            "layers_rendered": [s.get("layer_id") for s in layer_specs],
        }
        if url:
            result["url"] = url
            result["markdown"] = (
                f"[Открыть интерактивную карту]({url})"
            )
            result["hint_render"] = (
                "Сообщи пользователю прямую ссылку из поля url — "
                "HTML нельзя вставить как изображение."
            )
        if warnings_out:
            result["warnings"] = warnings_out

        return json.dumps(result, ensure_ascii=False, indent=2)

    return [plot_interactive]
