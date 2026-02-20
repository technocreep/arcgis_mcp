"""Tool: plot_layer — статичная карта одного слоя (matplotlib).

Главный визуализационный инструмент. Автоматически подбирает стиль
по типу геометрии и colormap по семантике данных (единицы измерения).
"""

from __future__ import annotations

import json
import time
from typing import Callable

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import numpy as np

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    prepare_for_plot,
    get_license_boundary,
    draw_license_boundary,
    get_license_view_bounds,
    clip_quantiles,
    make_title,
    make_colorbar_label,
    auto_colormap,
    save_figure,
    field_stats,
    upload_to_minio,
)

_SKIP_FIELDS = {"objectid", "fid", "shape_length", "shape_area", "globalid", "shape"}


def _auto_style(geometry_type: str, feature_count: int) -> str:
    gt = (geometry_type or "").lower()
    if "point" in gt:
        return "scatter" if feature_count > 500 else "markers"
    if "line" in gt or "string" in gt:
        return "lines"
    if "polygon" in gt:
        return "filled_polygons"
    return "scatter"


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError("Проект не выбран. Вызовите get_project_summary(project_id=...).")
        return pid

    def plot_layer(
        layer_id: str,
        project_id: str | None = None,
        color_field: str | None = None,
        style: str = "auto",
        colormap: str = "auto",
        show_license: bool = True,
        bbox_wgs84: str | None = None,
        title: str | None = None,
        output_format: str = "png",
    ) -> str:
        """Визуализировать один слой на статичной карте и сохранить PNG/SVG.

        Главный инструмент для отображения геофизических, геологических и других слоёв.
        Автоматически подбирает стиль рендеринга и colormap по единицам измерения из manifest.

        Args:
            layer_id: ID или display_name слоя из manifest.
            project_id: ID проекта (необязательно, если уже выбран через get_project_summary).
            color_field: Поле для раскраски. Числовое → colorbar, категориальное → легенда.
                         Если None — единый цвет (steelblue/lightblue/black).
            style: "auto" | "points" | "lines" | "polygons". Auto определяет по geometry_type.
            colormap: "auto" (мГал→RdYlBu_r, нТл→RdBu_r, высоты→terrain...) или имя matplotlib colormap.
            show_license: Рисовать контур лицензионного участка (по умолчанию True).
            bbox_wgs84: Обрезать по bbox: "minx,miny,maxx,maxy" в WGS84. Если None — авто-extent.
            title: Заголовок карты. None → автогенерация из display_name + units + field + n=count.
            output_format: "png" (по умолчанию) или "svg".
        """
        try:
            pid = _resolve_project(project_id)
            resolved_id = store.resolve_layer_name(pid, layer_id) or layer_id
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        entry = store.get_layer_entry(manifest, resolved_id) or {}

        try:
            gdf = load_and_reproject(gdb_path, resolved_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя '{resolved_id}': {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{resolved_id}' пустой."}, ensure_ascii=False)

        # Bbox-фильтр
        if bbox_wgs84:
            try:
                from shapely.geometry import box as _box
                minx, miny, maxx, maxy = map(float, bbox_wgs84.split(","))
                gdf = gdf[gdf.intersects(_box(minx, miny, maxx, maxy))].copy()
            except Exception:
                pass

        gdf, downsampled = prepare_for_plot(gdf)

        geom_type = entry.get("geometry_type") or (
            gdf.geometry.geom_type.mode().iloc[0] if len(gdf) > 0 else "Point"
        )
        feature_count = entry.get("feature_count", len(gdf))
        units = entry.get("units")
        display_name = entry.get("display_name", resolved_id)

        # Разрешить color_field
        resolved_color_field = None
        if color_field:
            if color_field in gdf.columns:
                resolved_color_field = color_field
            else:
                cl = color_field.lower()
                for col in gdf.columns:
                    if col.lower() == cl:
                        resolved_color_field = col
                        break

        # Авто-определение поля для раскраски геофизических слоёв
        if resolved_color_field is None:
            resolved_color_field = entry.get("default_color_field")
            if resolved_color_field is not None and resolved_color_field not in gdf.columns:
                resolved_color_field = None
        if resolved_color_field is None and units:
            # Геофизический слой с единицами → берём первое числовое нон-системное поле
            numeric_cols = [
                c for c in gdf.columns
                if c.lower() not in _SKIP_FIELDS and c != "geometry"
                and np.issubdtype(gdf[c].dtype, np.number)
            ]
            if numeric_cols:
                resolved_color_field = numeric_cols[0]

        # Colormap
        resolved_cmap = colormap if colormap != "auto" else auto_colormap(
            resolved_color_field, units, display_name
        )

        gt_lower = (geom_type or "").lower()
        resolved_style = style if style != "auto" else _auto_style(geom_type, feature_count)

        # Figsize: подобрать по соотношению сторон
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        dx = bounds[2] - bounds[0] or 1
        dy = bounds[3] - bounds[1] or 1
        ratio = dx / dy
        fig_w = 12
        fig_h = max(6, min(14, fig_w / max(ratio, 0.3)))
        fig, ax = plt.subplots(figsize=(fig_w, fig_h))

        # ----------------------------------------------------------------
        # Рендеринг
        # ----------------------------------------------------------------
        stats_dict = {}
        is_point = "point" in gt_lower or resolved_style in ("scatter", "markers", "points")
        is_line = "line" in gt_lower or "string" in gt_lower or resolved_style == "lines"

        if resolved_color_field and resolved_color_field in gdf.columns:
            col_series = gdf[resolved_color_field]
            is_numeric = np.issubdtype(col_series.dtype, np.number)
            stats_dict = field_stats(col_series)

            if is_numeric:
                vmin, vmax = clip_quantiles(col_series)

                if is_point:
                    sc = ax.scatter(
                        gdf.geometry.x, gdf.geometry.y,
                        c=col_series.values, cmap=resolved_cmap,
                        vmin=vmin, vmax=vmax,
                        s=4 if len(gdf) > 10_000 else (12 if len(gdf) > 2_000 else 25),
                        alpha=0.9, linewidths=0,
                    )
                    plt.colorbar(sc, ax=ax, label=make_colorbar_label(resolved_color_field, units), shrink=0.8)

                elif is_line:
                    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
                    cmap_obj = plt.cm.get_cmap(resolved_cmap)
                    for _, row in gdf.iterrows():
                        val = row[resolved_color_field]
                        try:
                            fval = float(val)
                            if not np.isnan(fval):
                                color = cmap_obj(norm(fval))
                                if row.geometry.geom_type == "LineString":
                                    xs, ys = row.geometry.xy
                                    ax.plot(xs, ys, color=color, linewidth=0.8)
                                else:
                                    for geom in row.geometry.geoms:
                                        xs, ys = geom.xy
                                        ax.plot(xs, ys, color=color, linewidth=0.8)
                        except (TypeError, ValueError):
                            pass
                    sm = plt.cm.ScalarMappable(cmap=resolved_cmap, norm=norm)
                    sm.set_array([])
                    plt.colorbar(sm, ax=ax, label=make_colorbar_label(resolved_color_field, units), shrink=0.8)

                else:
                    gdf.plot(
                        ax=ax, column=resolved_color_field, cmap=resolved_cmap,
                        vmin=vmin, vmax=vmax,
                        edgecolor="gray", linewidth=0.3, alpha=0.75, legend=True,
                    )

            else:
                # Категориальное
                unique_vals = col_series.dropna().astype(str).unique()
                cmap_obj = plt.cm.get_cmap("tab20", max(len(unique_vals), 1))
                color_map = {v: cmap_obj(i) for i, v in enumerate(unique_vals)}
                gdf["_color"] = col_series.astype(str).map(color_map)

                if is_point:
                    ax.scatter(
                        gdf.geometry.x, gdf.geometry.y,
                        c=list(gdf["_color"].fillna("gray")),
                        s=4 if len(gdf) > 10_000 else 20,
                        alpha=0.9, linewidths=0,
                    )
                elif is_line:
                    gdf.plot(ax=ax, color=gdf["_color"].fillna("gray"), linewidth=0.8, alpha=0.85)
                else:
                    gdf.plot(
                        ax=ax, color=gdf["_color"].fillna("lightgray"),
                        edgecolor="gray", linewidth=0.3, alpha=0.75,
                    )

                handles = [
                    Patch(facecolor=c, label=v)
                    for v, c in list(color_map.items())[:20]
                ]
                ax.legend(handles=handles, loc="upper right", fontsize=7,
                          title=resolved_color_field, title_fontsize=8)

        else:
            # Без поля — единый цвет
            if is_point:
                ax.scatter(gdf.geometry.x, gdf.geometry.y,
                           c="steelblue",
                           s=4 if len(gdf) > 10_000 else 20,
                           alpha=0.85, linewidths=0)
            elif is_line:
                gdf.plot(ax=ax, color="steelblue", linewidth=0.8, alpha=0.85)
            else:
                gdf.plot(ax=ax, color="lightblue", edgecolor="gray", linewidth=0.3, alpha=0.75)

        # ----------------------------------------------------------------
        # Контур лицензии
        # ----------------------------------------------------------------
        if show_license:
            lic_gdf = get_license_boundary(pid, store)
            draw_license_boundary(ax, lic_gdf)
            view_bounds = get_license_view_bounds(lic_gdf)
            if view_bounds:
                ax.set_xlim(view_bounds[0], view_bounds[2])
                ax.set_ylim(view_bounds[1], view_bounds[3])
            if lic_gdf is not None:
                ax.legend(loc="upper right", fontsize=8)

        auto_title = title or make_title(resolved_id, manifest, resolved_color_field)
        ax.set_title(auto_title, fontsize=13, fontweight="bold")
        ax.set_xlabel("Долгота, °E")
        ax.set_ylabel("Широта, °N")
        ax.set_aspect("equal")
        plt.tight_layout()

        safe_name = resolved_id.replace("/", "_")
        if resolved_color_field:
            safe_name += f"_{resolved_color_field}"
        out_path = save_figure(fig, pid, f"{safe_name}_{int(time.time())}", fmt=output_format)
        url = upload_to_minio(out_path, pid)

        result: dict = {
            "file": out_path,
            "url": url,
            "markdown": f"![{display_name}]({url})" if url else None,
            "layer": resolved_id,
            "display_name": display_name,
            "feature_count": len(gdf),
            "geometry_type": geom_type,
            "color_field": resolved_color_field,
            "colormap": resolved_cmap,
            "style": resolved_style,
        }
        if stats_dict:
            result["field_stats"] = stats_dict
        if downsampled:
            result["warning"] = (
                f"Слой содержит {feature_count:,} объектов. "
                f"Показаны 50,000 (случайная выборка)."
            )
        return json.dumps(result, ensure_ascii=False, indent=2)

    return [plot_layer]
