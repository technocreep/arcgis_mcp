"""Tool: plot_relief — изолинии рельефа с подписями высот, реками и контуром лицензии."""

from __future__ import annotations

import json
import time
from typing import Callable

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import box as _box

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    get_license_boundary,
    draw_license_boundary,
    get_license_view_bounds,
    save_figure,
    upload_to_minio,
)

_ELEV_CANDIDATES = [
    "phlr_abs", "cont", "contour", "elev", "elevation", "height", "z", "alt",
    "отметка", "высота", "горизонталь", "h",
]
_ELEV_SKIP = {
    "objectid", "fid", "shape_length", "shape_area", "globalid",
    "fnode_", "tnode_", "lpoly_", "rpoly_", "phlr_", "phlr_id",
}
_RIVER_KEYWORDS = ["river", "реки", "река", "hydro", "гидро", "water", "stream", "ручей"]


def _find_elevation_field(gdf) -> str | None:
    cols_lower = {c.lower(): c for c in gdf.columns if c.lower() != "geometry"}
    for c in _ELEV_CANDIDATES:
        if c not in cols_lower:
            continue
        col = cols_lower[c]
        series = gdf[col]
        if np.issubdtype(series.dtype, np.number):
            return col
        converted = pd.to_numeric(series, errors="coerce")
        if converted.notna().any():
            gdf[col] = converted
            return col
    for col in gdf.columns:
        if col.lower() in _ELEV_SKIP:
            continue
        if np.issubdtype(gdf[col].dtype, np.number):
            return col
    return None


def _label_isolines(ax, gdf, elev_col: str, view: tuple, target: int = 50):
    visible_count = int(gdf.intersects(_box(*view)).sum())
    every_n = max(1, round(visible_count / target))
    count = 0
    for _, row in gdf.iterrows():
        val = row.get(elev_col)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue
        count += 1
        if count % every_n != 0:
            continue
        mid = row.geometry.interpolate(0.5, normalized=True)
        ax.annotate(
            str(int(round(val))),
            xy=(mid.x, mid.y),
            fontsize=6, color="saddlebrown",
            ha="center", va="center",
            bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.65),
        )


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id):
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError("Проект не выбран. Вызовите get_project_summary(project_id=...).")
        return pid

    def plot_relief(
        layer_id: str,
        project_id: str | None = None,
        show_rivers: bool = True,
        show_license: bool = True,
        title: str | None = None,
        output_format: str = "png",
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

        elev_col = _find_elevation_field(gdf)

        # Контур лицензии — определяет видимую область
        lic_gdf = get_license_boundary(pid, store) if show_license else None
        view_bounds = get_license_view_bounds(lic_gdf)

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
            _label_isolines(ax, gdf, elev_col, view, target=50)

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

        return json.dumps({
            "file": out_path,
            "url": url,
            "markdown": f"![{display_name}]({url})" if url else None,
            "layer": resolved_id,
            "display_name": display_name,
            "elevation_field": elev_col,
            "feature_count": len(gdf),
        }, ensure_ascii=False, indent=2)

    return [plot_relief]
