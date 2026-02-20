"""Tool: plot_interactive — интерактивная карта (Folium + HTML).

Для навигации, tooltip'ов, переключения слоёв.
Ограничение: Folium плохо справляется с >500 объектами на слой.
Для тяжёлых слоёв используйте plot_layer.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    get_license_boundary,
    get_license_view_bounds,
    auto_tooltip_fields,
    upload_to_minio,
    DEFAULT_STYLES,
)

_MAX_FEATURES_DEFAULT = 500


def _geom_type_mode(gdf) -> str:
    if gdf.empty:
        return "Point"
    return gdf.geometry.geom_type.mode().iloc[0]


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError("Проект не выбран. Вызовите get_project_summary(project_id=...).")
        return pid

    def plot_interactive(
        layers: str,
        project_id: str | None = None,
        tooltip_fields: str | None = None,
        center: str | None = None,
        zoom: int = 10,
        max_features_per_layer: int = _MAX_FEATURES_DEFAULT,
        style_overrides: str | None = None,
    ) -> str:
        """Создать интерактивную HTML-карту (Folium) с переключением слоёв и tooltip'ами.

        Оптимален для небольших наборов данных (скважины, канавы, рудные точки).
        Для тяжёлых слоёв (>500 объектов) возвращает предупреждение и усекает данные.
        Возвращает путь к .html файлу.

        Args:
            layers: JSON-массив ID слоёв. Пример: '["Скважины_ГСК", "Канавы_ГСК", "river"]'.
                    Все слои автоматически конвертируются в WGS84.
            project_id: ID проекта (необязательно, если уже выбран).
            tooltip_fields: JSON-словарь {layer_id: [field1, field2, ...]}.
                            Если None — поля выбираются автоматически из manifest.
                            Пример: '{"Скважины_ГСК": ["Имя", "POINT_Z", "Участ"]}'
            center: Центр карты "[lat, lon]". None → авто-центр по данным.
            zoom: Начальный масштаб (по умолчанию 10).
            max_features_per_layer: Максимум объектов на слой (по умолчанию 500).
                                    Тяжёлые слои усекаются с предупреждением агенту.
            style_overrides: JSON-словарь переопределения стилей {layer_id: {color, weight, ...}}.
                             Пример: '{"river": {"color": "#4488ff", "weight": 1}}'
        """
        try:
            import folium
            from folium import plugins as folium_plugins
        except ImportError:
            return json.dumps({"error": "folium не установлен. Добавьте folium в requirements.txt."}, ensure_ascii=False)

        try:
            pid = _resolve_project(project_id)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        # Парсинг параметров
        try:
            layer_ids: list[str] = json.loads(layers)
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "Параметр layers: невалидный JSON."}, ensure_ascii=False)

        tooltip_map: dict[str, list[str]] = {}
        if tooltip_fields:
            try:
                tooltip_map = json.loads(tooltip_fields)
            except Exception:
                tooltip_map = {}

        style_map: dict[str, dict] = {}
        if style_overrides:
            try:
                style_map = json.loads(style_overrides)
            except Exception:
                style_map = {}

        center_coords: list | None = None
        if center:
            try:
                center_coords = json.loads(center)
            except Exception:
                pass

        # Загружаем слои
        loaded: dict[str, object] = {}  # layer_id → GeoDataFrame
        truncated_warnings: list[str] = []

        for raw_id in layer_ids:
            resolved_id = store.resolve_layer_name(pid, raw_id) or raw_id
            try:
                gdf = load_and_reproject(gdb_path, resolved_id)
            except Exception:
                continue
            if gdf.empty:
                continue

            if len(gdf) > max_features_per_layer:
                truncated_warnings.append(
                    f"Слой {resolved_id} содержит {len(gdf):,} объектов — "
                    f"показаны первые {max_features_per_layer}. "
                    f"Для полной визуализации используйте plot_layer."
                )
                gdf = gdf.head(max_features_per_layer)

            loaded[resolved_id] = gdf

        if not loaded:
            return json.dumps({"error": "Ни один слой не загружен."}, ensure_ascii=False)

        # Контур лицензии — определяет центр и zoom карты
        lic_gdf = get_license_boundary(pid, store)
        lic_view_bounds = get_license_view_bounds(lic_gdf)

        # Определяем центр карты
        if center_coords and len(center_coords) == 2:
            map_center = center_coords
        elif lic_gdf is not None and not lic_gdf.empty:
            b = lic_gdf.total_bounds
            map_center = [float((b[1] + b[3]) / 2), float((b[0] + b[2]) / 2)]
        else:
            import numpy as np
            all_bounds = [gdf.total_bounds for gdf in loaded.values()]
            arr = np.array(all_bounds)
            map_center = [
                float((arr[:, 1].min() + arr[:, 3].max()) / 2),
                float((arr[:, 0].min() + arr[:, 2].max()) / 2),
            ]

        m = folium.Map(location=map_center, zoom_start=zoom, tiles="CartoDB positron")

        # Подогнать карту по границам лицензии
        if lic_view_bounds:
            minx, miny, maxx, maxy = lic_view_bounds
            m.fit_bounds([[miny, minx], [maxy, maxx]])

        # Контур лицензии
        if lic_gdf is not None and not lic_gdf.empty:
            lic_group = folium.FeatureGroup(name="Контур лицензии", show=True)
            folium.GeoJson(
                lic_gdf.to_json(),
                style_function=lambda f: {
                    "color": "red", "weight": 2, "dashArray": "6 4", "fillOpacity": 0,
                },
                name="Контур лицензии",
            ).add_to(lic_group)
            lic_group.add_to(m)

        # Слои
        for resolved_id, gdf in loaded.items():
            entry = store.get_layer_entry(manifest, resolved_id) or {}
            display_name = entry.get("display_name", resolved_id)
            gt = _geom_type_mode(gdf)
            gt_lower = gt.lower()

            # Стиль
            override = style_map.get(resolved_id, {})
            def_style = DEFAULT_STYLES.get(gt, {})
            color = override.get("color", def_style.get("color", "steelblue"))
            weight = override.get("weight", def_style.get("linewidth", 1))
            fill_color = override.get("fillColor", color)
            fill_opacity = override.get("fillOpacity", 0.5 if "polygon" in gt_lower else 0)
            radius = override.get("radius", 6)

            # Tooltip поля
            tip_fields = tooltip_map.get(resolved_id) or auto_tooltip_fields(gdf, entry)

            fg = folium.FeatureGroup(name=display_name, show=True)

            if "point" in gt_lower:
                for _, row in gdf.iterrows():
                    try:
                        coords = [row.geometry.y, row.geometry.x]
                    except Exception:
                        continue
                    tip_html = "<br>".join(
                        f"<b>{f}</b>: {row.get(f, '')}"
                        for f in tip_fields if f in row.index
                    )
                    folium.CircleMarker(
                        location=coords,
                        radius=radius,
                        color=color,
                        fill=True,
                        fill_color=fill_color,
                        fill_opacity=0.8,
                        popup=folium.Popup(tip_html, max_width=300) if tip_html else None,
                        tooltip=str(row.get(tip_fields[0], resolved_id)) if tip_fields else None,
                    ).add_to(fg)

            else:
                # Line / Polygon → GeoJson
                tip_fields_in_df = [f for f in tip_fields if f in gdf.columns]

                def style_fn(feature, _color=color, _weight=weight,
                             _fill=fill_color, _fill_op=fill_opacity):
                    return {
                        "color": _color, "weight": _weight,
                        "fillColor": _fill, "fillOpacity": _fill_op,
                    }

                tooltip = (
                    folium.GeoJsonTooltip(fields=tip_fields_in_df)
                    if tip_fields_in_df else None
                )
                folium.GeoJson(
                    gdf.to_json(),
                    style_function=style_fn,
                    tooltip=tooltip,
                    name=display_name,
                ).add_to(fg)

            fg.add_to(m)

        folium.LayerControl(collapsed=False).add_to(m)

        # Сохранение
        from arcgis_mcp.config import PROJECTS_DIR
        viz_dir = Path(PROJECTS_DIR) / pid / "viz"
        viz_dir.mkdir(parents=True, exist_ok=True)
        out_path = viz_dir / f"interactive_{int(time.time())}.html"
        m.save(str(out_path))

        url = upload_to_minio(str(out_path), pid)

        result: dict = {
            "file": str(out_path),
            "url": url,
            "link": f"[Открыть интерактивную карту]({url})" if url else None,
            "layers_rendered": list(loaded.keys()),
            "map_center": map_center,
            "zoom": zoom,
        }
        if truncated_warnings:
            result["warnings"] = truncated_warnings

        return json.dumps(result, ensure_ascii=False, indent=2)

    return [plot_interactive]
