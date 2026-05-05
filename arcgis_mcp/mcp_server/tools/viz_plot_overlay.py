"""Tool: plot_overlay — наложение нескольких слоёв на одной карте (matplotlib).

Порядок рендеринга определяется автоматически по типу геометрии:
крупные полигоны → мелкие полигоны → density → линии → рельеф → точки → контур лицензии.
"""

from __future__ import annotations

import json
import time
from typing import Callable

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
from scipy.interpolate import griddata

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    prepare_for_plot,
    get_license_boundary,
    draw_license_boundary,
    get_license_view_bounds,
    clip_to_view,
    get_semantic_style,
    save_figure,
    upload_to_minio,
    DEFAULT_STYLES,
    find_elevation_field,
    label_isolines,
)

_RELIEF_KEYWORDS = ("relief", "рельеф", "изолин", "горизон", "contour")


def _sort_key(rec: dict):
    """Приоритет рендеринга: 0=крупные полигоны … 5=рельеф. Меньше = рисуется первым (снизу)."""
    gt_lower = rec["gt_lower"]
    if "polygon" in gt_lower:
        median_area = float(rec["gdf"].geometry.area.median())
        return (0, -median_area)       # крупные полигоны ниже мелких
    if rec["style_spec"] == "density":
        return (2, 0)
    if "line" in gt_lower or "string" in gt_lower:
        if rec["is_relief"]:
            return (4, 0)              # рельеф выше обычных линий
        return (3, 0)
    return (5, 0)                      # точки поверх всего


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        if not project_id:
            raise ValueError("project_id обязателен.")
        return project_id

    def plot_overlay(
        layers: str,
        project_id: str | None = None,
        show_license: bool = True,
        show_legend: bool = True,
        title: str | None = None,
        output_format: str = "png",
        license_margin: float = 0.20,
    ) -> str:
        """Наложить несколько слоёв на одну карту и сохранить PNG.

        Для сводных карт — геология + тектоника + скважины + геофизика и т.д.
        Порядок рендеринга определяется автоматически по типу геометрии:
        крупные полигоны → мелкие полигоны → density → линии → рельеф → точки → контур лицензии.
        Порядок слоёв в массиве на визуализацию не влияет.

        Args:
            layers: JSON-массив слоёв с параметрами стиля. Пример:
                '[{"layer_id": "relief",      "color": "brown", "linewidth": 0.2, "alpha": 0.3},
                  {"layer_id": "river",       "color": "#4488ff", "linewidth": 0.5},
                  {"layer_id": "Канавы_ГСК",  "color": "orange", "linewidth": 1.5},
                  {"layer_id": "Скважины_ГСК","color": "red",    "markersize": 15}]'

                Доступные ключи для каждого слоя:
                  layer_id (обязательно), color, alpha, linewidth, linestyle,
                  markersize, marker, edgecolor, label (для легенды),
                  style ("scatter"|"density"|"lines"|"polygons"),
                  color_field (поле для раскраски/интерполяции),
                  colormap (matplotlib colormap, напр. "RdBu_r").
                style="density" + color_field → интерполяция griddata + contourf для точечных слоёв.
                Рельефные слои (relief/горизонтали) рисуются серым с подписями высот автоматически.

            project_id: ID проекта (необязательно, если уже выбран).
            show_license: Рисовать контур лицензии последним (по умолчанию True).
            show_legend: Показывать легенду со списком слоёв (по умолчанию True).
            title: Заголовок карты. None → автогенерация.
            output_format: "png" или "svg".
            license_margin: Отступ вокруг контура лицензии — доля от max(ширина, высота).
                            0.20 по умолчанию. Увеличь до 0.4–0.5 для обзора соседних территорий.
        """
        try:
            pid = _resolve_project(project_id)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            layer_specs = json.loads(layers)
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "Параметр layers: невалидный JSON."}, ensure_ascii=False)

        if not isinstance(layer_specs, list) or not layer_specs:
            return json.dumps({"error": "layers должен быть непустым JSON-массивом."}, ensure_ascii=False)

        all_bounds: list = []

        # Контур лицензии — задаёт видимую область
        lic_gdf = get_license_boundary(pid, store) if show_license else None
        view_bounds = get_license_view_bounds(lic_gdf, margin=license_margin)

        # Размер фигуры из соотношения сторон view_bounds — чтобы set_aspect("equal")
        # не добавлял пустых полей сверху/снизу
        if view_bounds:
            _vdx = (view_bounds[2] - view_bounds[0]) or 1
            _vdy = (view_bounds[3] - view_bounds[1]) or 1
            _ratio = _vdx / _vdy
        else:
            _ratio = 14 / 12
        _fig_w = 12
        _fig_h = max(6, min(14, _fig_w / max(_ratio, 0.3)))
        fig, ax = plt.subplots(figsize=(_fig_w, _fig_h))

        # ── Проход 1: загрузка всех слоёв ────────────────────────────────────
        records: list[dict] = []

        for spec in layer_specs:
            if not isinstance(spec, dict) or "layer_id" not in spec:
                continue

            raw_id = spec["layer_id"]
            resolved_id = store.resolve_layer_name(pid, raw_id) or raw_id
            entry = store.get_layer_entry(manifest, resolved_id) or {}
            display_name = entry.get("display_name", resolved_id)
            label = spec.get("label", display_name)

            try:
                gdf = load_and_reproject(gdb_path, resolved_id)
            except Exception:
                continue

            if gdf.empty:
                continue

            if view_bounds:
                gdf = clip_to_view(gdf, view_bounds)
                if gdf.empty:
                    continue

            gdf, _ = prepare_for_plot(gdf, max_features=50_000)
            all_bounds.append(gdf.total_bounds)

            gt = gdf.geometry.geom_type.mode().iloc[0] if len(gdf) > 0 else "Point"
            gt_key = gt if gt in DEFAULT_STYLES else "Point"
            semantic = get_semantic_style(resolved_id, display_name, entry.get("feature_dataset"))
            base = semantic or DEFAULT_STYLES.get(gt_key, {})

            gt_lower = gt.lower()
            style_spec = spec.get("style", "auto")
            # is_relief только когда стиль не задан явно
            is_relief = (
                style_spec in ("auto", "scatter")
                and any(kw in resolved_id.lower() or kw in display_name.lower()
                        for kw in _RELIEF_KEYWORDS)
            )

            records.append({
                "spec": spec,
                "resolved_id": resolved_id,
                "display_name": display_name,
                "label": label,
                "gdf": gdf,
                "gt_lower": gt_lower,
                "is_relief": is_relief,
                "style_spec": style_spec,
                "color_field": spec.get("color_field"),
                "cmap_spec": spec.get("colormap", "viridis"),
                "color":     spec.get("color",     base.get("color", "steelblue")),
                "alpha":     spec.get("alpha",     base.get("alpha", 0.85)),
                "linewidth": spec.get("linewidth", base.get("linewidth", 1.0)),
                "linestyle": spec.get("linestyle", base.get("linestyle", "-")),
                "markersize":spec.get("markersize",base.get("markersize", 10)),
                "marker":    spec.get("marker",    base.get("marker", "o")),
                "edgecolor": spec.get("edgecolor", base.get("edgecolor", "none")),
            })

        if not records:
            plt.close(fig)
            return json.dumps({"error": "Ни один слой не загружен успешно."}, ensure_ascii=False)

        # ── Сортировка: крупные полигоны снизу … точки сверху ────────────────
        records.sort(key=_sort_key)

        # ── Проход 2: рендеринг в отсортированном порядке ────────────────────
        legend_handles: list = []
        loaded_layers: list[str] = []

        for zorder, rec in enumerate(records, start=1):
            gdf         = rec["gdf"]
            gt_lower    = rec["gt_lower"]
            is_relief   = rec["is_relief"]
            style_spec  = rec["style_spec"]
            color_field = rec["color_field"]
            cmap_spec   = rec["cmap_spec"]
            color       = rec["color"]
            alpha       = rec["alpha"]
            linewidth   = rec["linewidth"]
            linestyle   = rec["linestyle"]
            markersize  = rec["markersize"]
            marker      = rec["marker"]
            edgecolor   = rec["edgecolor"]
            label       = rec["label"]
            resolved_id = rec["resolved_id"]

            loaded_layers.append(resolved_id)

            if "point" in gt_lower:
                if style_spec == "density":
                    _x = gdf.geometry.x.values
                    _y = gdf.geometry.y.values

                    # Найти колонку нечувствительно к пробелам
                    actual_col = None
                    if color_field:
                        cf_norm = " ".join(color_field.split()).lower()
                        for col in gdf.columns:
                            if " ".join(col.split()).lower() == cf_norm:
                                actual_col = col
                                break

                    if actual_col is not None:
                        col_vals = gdf[actual_col].values.astype(float)
                        _mask = ~np.isnan(col_vals)
                        _x, _y, _z = _x[_mask], _y[_mask], col_vals[_mask]
                        _dx = (_x.max() - _x.min()) or 1
                        if view_bounds:
                            _lic_dx = (view_bounds[2] - view_bounds[0]) or _dx
                            _ig = min(500, max(200, round(300 * _dx / _lic_dx)))
                        else:
                            _ig = 300
                        gx, gy = np.mgrid[
                            _x.min():_x.max():complex(_ig),
                            _y.min():_y.max():complex(_ig),
                        ]
                        gz = griddata((_x, _y), _z, (gx, gy), method="linear")
                        cf = ax.contourf(gx, gy, gz, levels=20, cmap=cmap_spec, zorder=zorder)
                        plt.colorbar(cf, ax=ax, label=actual_col, shrink=0.8)
                        ax.contour(gx, gy, gz, levels=10, colors="black",
                                   linewidths=0.4, alpha=0.5, zorder=zorder)
                    else:
                        # Нет color_field — KDE по плотности точек
                        from scipy.stats import gaussian_kde
                        _mask = ~(np.isnan(_x) | np.isnan(_y))
                        _x, _y = _x[_mask], _y[_mask]
                        if len(_x) >= 4:
                            kde = gaussian_kde(np.vstack([_x, _y]))
                            _dx = (_x.max() - _x.min()) or 1
                            _dy = (_y.max() - _y.min()) or 1
                            _ig = min(300, max(100, round(200 * _dx / max(_dy, _dx))))
                            gx, gy = np.mgrid[
                                _x.min():_x.max():complex(_ig),
                                _y.min():_y.max():complex(_ig),
                            ]
                            gz = kde(np.vstack([gx.ravel(), gy.ravel()])).reshape(gx.shape)
                            cf = ax.contourf(gx, gy, gz, levels=15, cmap=cmap_spec,
                                             alpha=alpha, zorder=zorder)
                            plt.colorbar(cf, ax=ax, label="density", shrink=0.8)
                    legend_handles.append(
                        Line2D([0], [0], color="gray", linewidth=6, label=label)
                    )
                else:
                    ax.scatter(
                        gdf.geometry.x, gdf.geometry.y,
                        c=color, s=markersize, marker=marker,
                        alpha=alpha, linewidths=0.3, edgecolors=edgecolor, zorder=zorder,
                    )
                    legend_handles.append(
                        Line2D([0], [0], marker=marker, color="w",
                               markerfacecolor=color, markersize=8, label=label)
                    )

            elif "line" in gt_lower or "string" in gt_lower:
                if is_relief:
                    gdf.plot(ax=ax, color="#888888", linewidth=0.5, alpha=0.5, zorder=zorder)
                    elev_col = find_elevation_field(gdf)
                    if elev_col and view_bounds:
                        label_isolines(ax, gdf, elev_col, view_bounds, target=50)
                    legend_handles.append(
                        Line2D([0], [0], color="#888888", linewidth=2, label=label)
                    )
                else:
                    gdf.plot(ax=ax, color=color, linewidth=linewidth,
                             linestyle=linestyle, alpha=alpha, zorder=zorder)
                    legend_handles.append(
                        Line2D([0], [0], color=color, linewidth=2, linestyle=linestyle, label=label)
                    )

            else:  # polygons
                gdf.plot(ax=ax, color=color, edgecolor=edgecolor,
                         linewidth=linewidth, alpha=alpha, zorder=zorder)
                legend_handles.append(
                    Patch(facecolor=color, edgecolor=edgecolor or "gray", label=label)
                )

        # Контур лицензии поверх всех слоёв
        lic_zorder = len(records) + 10
        if show_license and lic_gdf is not None:
            draw_license_boundary(ax, lic_gdf, zorder=lic_zorder)
            legend_handles.append(
                Line2D([0], [0], color="red", linewidth=2, linestyle="--", label="Контур лицензии")
            )

        if show_legend and legend_handles:
            ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

        if view_bounds:
            ax.set_xlim(view_bounds[0], view_bounds[2])
            ax.set_ylim(view_bounds[1], view_bounds[3])
        elif all_bounds:
            all_bounds_arr = np.array(all_bounds)
            ax.set_xlim(all_bounds_arr[:, 0].min(), all_bounds_arr[:, 2].max())
            ax.set_ylim(all_bounds_arr[:, 1].min(), all_bounds_arr[:, 3].max())

        auto_title = title or f"Карта: {', '.join(loaded_layers[:4])}"
        ax.set_title(auto_title, fontsize=13, fontweight="bold")
        ax.set_xlabel("Долгота, °E")
        ax.set_ylabel("Широта, °N")
        ax.set_aspect("equal")
        plt.tight_layout()

        out_name = f"overlay_{int(time.time())}"
        out_path = save_figure(fig, pid, out_name, fmt=output_format)
        url = upload_to_minio(out_path, pid)

        rendered_names = [r["display_name"] for r in records]
        if url:
            result = {"markdown": f"![Карта]({url})", "layers_rendered": rendered_names,
                      "hint_render": "Вставь значение поля markdown дословно в ответ — это готовая Markdown-ссылка на изображение."}
        else:
            result = {"file": out_path, "layers_rendered": rendered_names}
        return json.dumps(result, ensure_ascii=False, indent=2)

    return [plot_overlay]
