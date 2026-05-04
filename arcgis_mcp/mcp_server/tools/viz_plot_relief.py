"""Tool: plot_relief — изолинии рельефа с подписями высот, реками и контуром лицензии."""

from __future__ import annotations

import json
import time
from typing import Callable

import matplotlib.pyplot as plt

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    get_license_boundary,
    draw_license_boundary,
    get_license_view_bounds,
    save_figure,
    upload_to_minio,
    find_elevation_field,
    label_isolines,
)

_RIVER_KEYWORDS = ["river", "реки", "река", "hydro", "гидро", "water", "stream", "ручей"]


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id):
        if not project_id:
            raise ValueError("project_id обязателен.")
        return project_id

    def plot_relief(
        layer_id: str,
        project_id: str | None = None,
        show_rivers: bool = True,
        show_license: bool = True,
        title: str | None = None,
        output_format: str = "png",
        license_margin: float = 0.20,
    ) -> str:
        """Построить карту изолиний рельефа с подписями высот, реками и контуром лицензии.

        Специализированный инструмент для отображения горизонталей рельефа.
        Автоматически определяет поле высоты, находит слои рек и контур лицензии.
        Линии рельефа отображаются серым цветом, подписи высот расставляются
        адаптивно по плотности изолиний в видимой области.

        Args:
            layer_id: ID или display_name слоя горизонталей рельефа.
            project_id: ID проекта (необязательно, если уже выбран через get_project_summary).
            show_rivers: Отображать слои рек поверх рельефа (по умолчанию True).
            show_license: Рисовать контур лицензионного участка (по умолчанию True).
            title: Заголовок карты. None → автогенерация.
            output_format: "png" (по умолчанию) или "svg".
            license_margin: Отступ вокруг контура лицензии — доля от max(ширина, высота).
                            0.20 по умолчанию. Увеличь до 0.4–0.5 для обзора соседних территорий.
        """
        try:
            pid = _resolve_project(project_id)
            resolved_id = store.resolve_layer_name(pid, layer_id) or layer_id
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            gdf = load_and_reproject(gdb_path, resolved_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя '{resolved_id}': {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{resolved_id}' пустой."}, ensure_ascii=False)

        elev_col = find_elevation_field(gdf)

        # Контур лицензии — определяет видимую область
        lic_gdf = get_license_boundary(pid, store) if show_license else None
        view_bounds = get_license_view_bounds(lic_gdf, margin=license_margin)

        if view_bounds:
            view = view_bounds
        else:
            b = gdf.total_bounds
            view = (float(b[0]), float(b[1]), float(b[2]), float(b[3]))

        vdx = (view[2] - view[0]) or 1
        vdy = (view[3] - view[1]) or 1
        fig_w = 12
        fig_h = max(6, min(14, fig_w / max(vdx / vdy, 0.3)))
        fig, ax = plt.subplots(figsize=(fig_w, fig_h))

        # Рельеф — серые линии + подписи высот
        gdf.plot(ax=ax, color="#888888", linewidth=0.5, alpha=0.5, zorder=4)
        if elev_col:
            label_isolines(ax, gdf, elev_col, view, target=50)

        # Реки
        if show_rivers:
            all_layers = manifest.get("layers", [])
            for layer_entry in all_layers:
                lid = layer_entry.get("layer_id", "")
                dn = layer_entry.get("display_name", "")
                if any(kw in lid.lower() or kw in dn.lower() for kw in _RIVER_KEYWORDS):
                    try:
                        rgdf = load_and_reproject(gdb_path, lid)
                        if not rgdf.empty:
                            rgdf.plot(ax=ax, color="steelblue", linewidth=1.2,
                                      alpha=1.0, zorder=5)
                    except Exception:
                        pass

        # Контур лицензии поверх всего
        if show_license:
            draw_license_boundary(ax, lic_gdf)
            if lic_gdf is not None:
                ax.legend(loc="upper right", fontsize=8)

        ax.set_xlim(view[0], view[2])
        ax.set_ylim(view[1], view[3])

        entry = store.get_layer_entry(manifest, resolved_id) or {}
        display_name = entry.get("display_name", resolved_id)
        ax.set_title(title or f"Рельеф — {display_name}", fontsize=13, fontweight="bold")
        ax.set_xlabel("Долгота, °E")
        ax.set_ylabel("Широта, °N")
        ax.set_aspect("equal")
        plt.tight_layout()

        out_path = save_figure(fig, pid, f"relief_{int(time.time())}", fmt=output_format)
        url = upload_to_minio(out_path, pid)

        if url:
            result = {"markdown": f"![{display_name}]({url})", "display_name": display_name, "elevation_field": elev_col,
                      "hint_render": "Вставь значение поля markdown дословно в ответ — это готовая Markdown-ссылка на изображение."}
        else:
            result = {"file": out_path, "display_name": display_name, "elevation_field": elev_col}
        return json.dumps(result, ensure_ascii=False, indent=2)

    return [plot_relief]
