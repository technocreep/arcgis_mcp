"""Tool: plot_overlay — наложение нескольких слоёв на одной карте (matplotlib).

Для сводных карт: порядок слоёв в массиве = порядок рендеринга
(первый = подложка, последний = поверх).
"""

from __future__ import annotations

import json
import time
from typing import Callable

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    prepare_for_plot,
    get_license_boundary,
    draw_license_boundary,
    get_license_view_bounds,
    clip_to_view,
    clip_quantiles,
    make_colorbar_label,
    auto_colormap,
    get_semantic_style,
    save_figure,
    upload_to_minio,
    DEFAULT_STYLES,
)


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError("Проект не выбран. Вызовите get_project_summary(project_id=...).")
        return pid

    def plot_overlay(
        layers: str,
        project_id: str | None = None,
        show_license: bool = True,
        show_legend: bool = True,
        title: str | None = None,
        output_format: str = "png",
    ) -> str:
        """Наложить несколько слоёв на одну карту и сохранить PNG.

        Для сводных карт — геология + тектоника + скважины + геофизика и т.д.
        Первый слой в массиве = подложка, последний = поверх. Контур лицензии
        рисуется последним (zorder=10).

        Args:
            layers: JSON-массив слоёв с параметрами стиля. Пример:
                '[{"layer_id": "relief",      "color": "brown", "linewidth": 0.2, "alpha": 0.3},
                  {"layer_id": "river",       "color": "#4488ff", "linewidth": 0.5},
                  {"layer_id": "Канавы_ГСК",  "color": "orange", "linewidth": 1.5},
                  {"layer_id": "Скважины_ГСК","color": "red",    "markersize": 15}]'

                Доступные ключи для каждого слоя:
                  layer_id (обязательно), color, alpha, linewidth, linestyle,
                  markersize, marker, edgecolor, label (для легенды).
                Если color не указан — авто по типу геометрии.

            project_id: ID проекта (необязательно, если уже выбран).
            show_license: Рисовать контур лицензии последним (по умолчанию True).
            show_legend: Показывать легенду со списком слоёв (по умолчанию True).
            title: Заголовок карты. None → автогенерация.
            output_format: "png" или "svg".
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

        fig, ax = plt.subplots(figsize=(14, 12))

        legend_handles: list = []
        loaded_layers: list[str] = []
        all_bounds: list = []

        # Загружаем контур лицензии заранее — он определяет extent карты
        lic_gdf = get_license_boundary(pid, store) if show_license else None
        view_bounds = get_license_view_bounds(lic_gdf)

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
            except Exception as e:
                continue  # пропустить недоступный слой

            if gdf.empty:
                continue

            # Обрезать слой по границам лицензии (если они есть)
            if view_bounds:
                gdf = clip_to_view(gdf, view_bounds)
                if gdf.empty:
                    continue

            gdf, _ = prepare_for_plot(gdf, max_features=50_000)
            all_bounds.append(gdf.total_bounds)
            loaded_layers.append(resolved_id)

            gt = gdf.geometry.geom_type.mode().iloc[0] if len(gdf) > 0 else "Point"
            gt_key = gt if gt in DEFAULT_STYLES else "Point"

            # Семантический стиль имеет приоритет над геометрическим дефолтом
            semantic = get_semantic_style(resolved_id, display_name, entry.get("feature_dataset"))
            base = semantic or DEFAULT_STYLES.get(gt_key, {})

            # Параметры из spec перезаписывают base (agent override)
            color     = spec.get("color",     base.get("color", "steelblue"))
            alpha     = spec.get("alpha",     base.get("alpha", 0.85))
            linewidth = spec.get("linewidth", base.get("linewidth", 1.0))
            linestyle = spec.get("linestyle", base.get("linestyle", "-"))
            markersize= spec.get("markersize", base.get("markersize", 10))
            marker    = spec.get("marker",    base.get("marker", "o"))
            edgecolor = spec.get("edgecolor", base.get("edgecolor", "none"))

            gt_lower = gt.lower()

            if "point" in gt_lower:
                ax.scatter(
                    gdf.geometry.x, gdf.geometry.y,
                    c=color, s=markersize, marker=marker,
                    alpha=alpha, linewidths=0.3, edgecolors=edgecolor, zorder=3,
                )
                legend_handles.append(
                    Line2D([0], [0], marker=marker, color="w",
                           markerfacecolor=color, markersize=8, label=label)
                )

            elif "line" in gt_lower or "string" in gt_lower:
                gdf.plot(ax=ax, color=color, linewidth=linewidth,
                         linestyle=linestyle, alpha=alpha, zorder=3)
                legend_handles.append(
                    Line2D([0], [0], color=color, linewidth=2, linestyle=linestyle, label=label)
                )

            else:
                gdf.plot(ax=ax, color=color, edgecolor=edgecolor,
                         linewidth=linewidth, alpha=alpha, zorder=2)
                legend_handles.append(
                    Patch(facecolor=color, edgecolor=edgecolor or "gray", label=label)
                )

        if not loaded_layers:
            plt.close(fig)
            return json.dumps({"error": "Ни один слой не загружен успешно."}, ensure_ascii=False)

        # Контур лицензии последним (поверх всех слоёв)
        if show_license and lic_gdf is not None:
            draw_license_boundary(ax, lic_gdf)
            legend_handles.append(
                Line2D([0], [0], color="red", linewidth=2, linestyle="--", label="Контур лицензии")
            )

        if show_legend and legend_handles:
            ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

        # Extent: сначала по контуру лицензии, иначе — по всем слоям
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

        return json.dumps({
            "file": out_path,
            "url": url,
            "markdown": f"![Карта]({url})" if url else None,
            "layers_rendered": loaded_layers,
            "layers_requested": len(layer_specs),
        }, ensure_ascii=False, indent=2)

    return [plot_overlay]
