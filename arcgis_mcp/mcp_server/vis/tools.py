"""Универсальные инструменты визуализации для GIS MCP Server.

Три обобщённых инструмента, покрывающих все паттерны из ноутбука:

  visualize_layer   — пространственная карта слоя (Point/Line/Polygon),
                      опциональные оверлеи, раскраска по полю.
  plot_statistics   — атрибутивные графики (histogram, bar, pie, scatter, profile).
  interpolate_field — интерполяция точечного поля в растровую сетку (геофизика).

Все инструменты:
  - используют fuzzy-поиск слоя через store.resolve_layer_name()
  - читают .gdb через geopandas
  - сохраняют PNG в PROJECTS_DIR/{project_id}/vis_output/
  - возвращают JSON-строку (как все остальные инструменты сервера)
  - никогда не бросают исключений наружу
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

import matplotlib
matplotlib.use("Agg")  # headless — обязательно до импорта pyplot
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import numpy as np
import geopandas as gpd

from ..project_store import ProjectStore


# ---------------------------------------------------------------------------
# Вспомогательные утилиты
# ---------------------------------------------------------------------------

_SKIP_FIELDS = {"objectid", "fid", "shape_length", "shape_area", "globalid", "rel_globalid"}


def _auto_field(gdf: gpd.GeoDataFrame, preferred: str | None) -> str | None:
    """Выбрать поле для цветового кодирования/оси.

    Порядок: preferred → первое числовое → первое строковое.
    Системные поля (OBJECTID и т.п.) пропускаются.
    """
    cols = [c for c in gdf.columns if c.lower() not in _SKIP_FIELDS and c != "geometry"]
    if preferred and preferred in gdf.columns:
        return preferred
    # Числовые
    num_cols = [c for c in cols if np.issubdtype(gdf[c].dtype, np.number)]
    if num_cols:
        return num_cols[0]
    # Строковые
    return cols[0] if cols else None


def _is_numeric(series) -> bool:
    return np.issubdtype(series.dtype, np.number)


def _save_figure(fig: plt.Figure, out_dir: Path, name: str) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    path = out_dir / f"{name}_{ts}.png"
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return str(path)


def _categorical_colors(series) -> dict:
    """Словарь {value: color} для категориального поля (≤ 30 значений)."""
    unique = series.dropna().astype(str).unique()
    cmap = plt.cm.get_cmap("tab20", max(len(unique), 1))
    return {v: cmap(i) for i, v in enumerate(unique)}


def _draw_layer(ax: plt.Axes, gdf: gpd.GeoDataFrame, color_by: str | None, **style) -> None:
    """Нарисовать один GeoDataFrame на осях с учётом типа геометрии."""
    if gdf.empty:
        return

    geom_type = gdf.geometry.geom_type.dropna().mode()
    gt = geom_type.iloc[0].lower() if len(geom_type) > 0 else "unknown"

    if "point" in gt:
        _draw_points(ax, gdf, color_by, **style)
    elif "line" in gt or "string" in gt:
        _draw_lines(ax, gdf, color_by, **style)
    else:
        _draw_polygons(ax, gdf, color_by, **style)


def _draw_points(ax, gdf, color_by, **style):
    xs = gdf.geometry.x.values
    ys = gdf.geometry.y.values
    s = style.get("markersize", 20)
    marker = style.get("marker", "o")
    edgecolor = style.get("edgecolor", "black")
    linewidth = style.get("linewidth", 0.4)
    alpha = style.get("alpha", 0.85)

    if color_by and color_by in gdf.columns:
        col = gdf[color_by]
        if _is_numeric(col):
            sc = ax.scatter(xs, ys, c=col.values, cmap="viridis", s=s,
                            marker=marker, edgecolors=edgecolor, linewidths=linewidth, alpha=alpha)
            plt.colorbar(sc, ax=ax, label=color_by, shrink=0.7)
        else:
            cm = _categorical_colors(col)
            colors = col.astype(str).map(cm).fillna("gray").values
            ax.scatter(xs, ys, c=list(colors), s=s, marker=marker,
                       edgecolors=edgecolor, linewidths=linewidth, alpha=alpha)
            # Легенда
            handles = [
                plt.Line2D([0], [0], marker=marker, color="w",
                           markerfacecolor=c, markersize=7, label=v)
                for v, c in list(cm.items())[:15]
            ]
            ax.legend(handles=handles, loc="upper right", fontsize=7, title=color_by, title_fontsize=8)
    else:
        color = style.get("color", "steelblue")
        ax.scatter(xs, ys, c=color, s=s, marker=marker,
                   edgecolors=edgecolor, linewidths=linewidth, alpha=alpha)


def _draw_lines(ax, gdf, color_by, **style):
    linewidth = style.get("linewidth", 1.2)
    alpha = style.get("alpha", 0.8)

    if color_by and color_by in gdf.columns and not _is_numeric(gdf[color_by]):
        cm = _categorical_colors(gdf[color_by])
        for val, grp in gdf.groupby(color_by):
            grp.plot(ax=ax, color=cm.get(str(val), "gray"), linewidth=linewidth, alpha=alpha,
                     label=str(val))
        ax.legend(loc="upper right", fontsize=7, title=color_by)
    else:
        color = style.get("color", "black")
        linestyle = style.get("linestyle", "-")
        gdf.plot(ax=ax, color=color, linewidth=linewidth, linestyle=linestyle, alpha=alpha)


def _draw_polygons(ax, gdf, color_by, **style):
    alpha = style.get("alpha", 0.6)
    edgecolor = style.get("edgecolor", "gray")
    linewidth = style.get("linewidth", 0.4)

    if color_by and color_by in gdf.columns:
        col = gdf[color_by]
        if _is_numeric(col):
            gdf.plot(ax=ax, column=color_by, cmap="viridis", edgecolor=edgecolor,
                     linewidth=linewidth, alpha=alpha, legend=True)
        else:
            cm = _categorical_colors(col)
            gdf["_color"] = col.astype(str).map(cm)
            gdf.plot(ax=ax, color=gdf["_color"].fillna("lightgray"),
                     edgecolor=edgecolor, linewidth=linewidth, alpha=alpha)
            handles = [Patch(facecolor=c, label=v) for v, c in list(cm.items())[:15]]
            ax.legend(handles=handles, loc="upper right", fontsize=7, title=color_by)
    else:
        color = style.get("color", "#AACCEE")
        gdf.plot(ax=ax, color=color, edgecolor=edgecolor, linewidth=linewidth, alpha=alpha)


# ---------------------------------------------------------------------------
# make_tools
# ---------------------------------------------------------------------------

def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    """Вернуть список визуализационных инструментов."""

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите get_project_summary(project_id=...)."
            )
        return pid

    def _resolve_layer(pid: str, layer: str):
        layer_id = store.resolve_layer_name(pid, layer)
        if layer_id is None:
            raise ValueError(
                f"Слой '{layer}' не найден. Используй list_layers() чтобы увидеть доступные слои."
            )
        return layer_id

    def _out_dir(pid: str, custom: str | None) -> Path:
        if custom:
            return Path(custom)
        from arcgis_mcp.config import PROJECTS_DIR
        return Path(PROJECTS_DIR) / pid / "vis_output"

    # -----------------------------------------------------------------------
    # visualize_layer
    # -----------------------------------------------------------------------

    def visualize_layer(
        layer: str,
        color_by: str | None = None,
        title: str | None = None,
        overlay_layers: str | None = None,
        label_field: str | None = None,
        output_dir: str | None = None,
        project_id: str | None = None,
    ) -> str:
        """Нарисовать слой на пространственной карте и сохранить PNG.

        Автоматически определяет тип геометрии (Point / Line / Polygon)
        и выбирает способ отображения. Поддерживает раскраску по атрибуту
        (числовое поле → colorbar, категориальное → легенда).
        Дополнительные слои задаются через overlay_layers.

        Args:
            layer: Название слоя (display_name, layer_id или alias).
            color_by: Поле для цветового кодирования. Если не указано — однотонная заливка.
                      Числовые поля → colormap viridis. Категориальные → tab20.
            title: Заголовок карты (автоматически, если не указан).
            overlay_layers: JSON-список оверлеев. Пример:
                '[{"layer":"tect_lines","color":"black","linewidth":1.5},
                  {"layer":"ore_points","color":"gold","marker":"D"}]'
            label_field: Поле для подписи объектов (None = без подписей).
                         Внимание: при большом числе объектов подписи перекроются.
            output_dir: Директория для сохранения PNG (по умолчанию: проект/vis_output/).
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            layer_id = _resolve_layer(pid, layer)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
            entry = store.get_layer_entry(manifest, layer_id) or {}
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            gdf = gpd.read_file(gdb_path, layer=layer_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя: {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{layer_id}' пустой (0 объектов)."}, ensure_ascii=False)

        # Авто-выбор поля раскраски
        resolved_color_by = _auto_field(gdf, color_by)

        fig, ax = plt.subplots(figsize=(12, 10))

        # Основной слой
        _draw_layer(ax, gdf, resolved_color_by)

        # Оверлеи
        overlays_loaded = []
        if overlay_layers:
            try:
                overlay_specs = json.loads(overlay_layers)
            except json.JSONDecodeError:
                overlay_specs = []

            for spec in overlay_specs:
                ol_name = spec.pop("layer", None)
                if not ol_name:
                    continue
                try:
                    ol_id = _resolve_layer(pid, ol_name)
                    ol_gdf = gpd.read_file(gdb_path, layer=ol_id)
                    ol_color_by = spec.pop("color_by", None)
                    _draw_layer(ax, ol_gdf, ol_color_by, **spec)
                    overlays_loaded.append(ol_id)
                except Exception:
                    pass

        # Подписи
        if label_field and label_field in gdf.columns:
            n = min(len(gdf), 300)  # ограничение числа подписей
            for _, row in gdf.head(n).iterrows():
                try:
                    c = row.geometry.centroid if row.geometry.geom_type != "Point" else row.geometry
                    ax.annotate(
                        str(row[label_field]), (c.x, c.y),
                        fontsize=5, ha="left", va="bottom",
                        xytext=(2, 2), textcoords="offset points",
                    )
                except Exception:
                    pass

        display_name = entry.get("display_name", layer_id)
        units = entry.get("units", "")
        map_title = title or f"{display_name}" + (f" ({units})" if units else "")

        ax.set_title(map_title, fontsize=13, fontweight="bold")
        ax.set_xlabel("Долгота, °E")
        ax.set_ylabel("Широта, °N")
        ax.set_aspect("equal")
        plt.tight_layout()

        out_path = _save_figure(fig, _out_dir(pid, output_dir), layer_id)

        return json.dumps({
            "file": out_path,
            "layer": layer_id,
            "display_name": display_name,
            "feature_count": len(gdf),
            "color_by": resolved_color_by,
            "overlays": overlays_loaded,
            "hint": f"Файл сохранён: {out_path}",
        }, ensure_ascii=False, indent=2)

    # -----------------------------------------------------------------------
    # plot_statistics
    # -----------------------------------------------------------------------

    def plot_statistics(
        layer: str,
        field: str | None = None,
        chart_type: str = "histogram",
        field2: str | None = None,
        group_by: str | None = None,
        limit: int = 2000,
        title: str | None = None,
        output_dir: str | None = None,
        project_id: str | None = None,
    ) -> str:
        """Построить атрибутивный график по полю слоя и сохранить PNG.

        Универсальный инструмент для статистической визуализации данных.
        Покрывает гистограммы, столбчатые диаграммы, круговые диаграммы,
        scatter-графики и профили вдоль оси.

        Args:
            layer: Название слоя (display_name, layer_id или alias).
            field: Поле для анализа. Если не указано — первое числовое поле слоя.
            chart_type: Тип диаграммы:
                "histogram" — распределение числового поля (с mean/std линиями).
                "bar"       — top-20 значений категориального поля (горизонт. барчарт).
                "pie"       — круговая диаграмма значений поля.
                "scatter"   — scatter-plot: field (ось X) vs field2 (ось Y).
                              Требует field2.
                "profile"   — значения field по пространственной оси.
                              field2 = "lat" (по широте) или "lon" (по долготе).
            field2: Второй аргумент: для scatter — Y-ось, для profile — "lat" или "lon".
            group_by: Для "bar"/"pie": группировать по этому полю вместо field.
            limit: Максимум объектов для загрузки из .gdb (по умолчанию 2000).
            title: Заголовок (автогенерация, если не указан).
            output_dir: Директория сохранения PNG.
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            layer_id = _resolve_layer(pid, layer)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
            entry = store.get_layer_entry(manifest, layer_id) or {}
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            gdf = gpd.read_file(gdb_path, layer=layer_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя: {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{layer_id}' пустой."}, ensure_ascii=False)

        if len(gdf) > limit:
            gdf = gdf.sample(limit, random_state=42)
            truncated = True
        else:
            truncated = False

        # Авто-определение поля
        resolved_field = _auto_field(gdf, field)
        if resolved_field is None:
            return json.dumps({"error": "Нет подходящих полей для визуализации."}, ensure_ascii=False)

        display_name = entry.get("display_name", layer_id)
        units = entry.get("units", "")

        fig, ax = plt.subplots(figsize=(10, 6))
        chart_type = chart_type.lower().strip()

        try:
            if chart_type == "histogram":
                series = gdf[resolved_field].dropna()
                if not _is_numeric(series):
                    return json.dumps({"error": f"Поле '{resolved_field}' не числовое — используй chart_type='bar'."}, ensure_ascii=False)
                ax.hist(series, bins=30, color="steelblue", edgecolor="white", alpha=0.85)
                ax.axvline(series.mean(), color="red", linestyle="--",
                           label=f"Среднее: {series.mean():.4g}")
                ax.axvline(series.median(), color="orange", linestyle="--",
                           label=f"Медиана: {series.median():.4g}")
                ax.set_xlabel(f"{resolved_field}" + (f" ({units})" if units else ""))
                ax.set_ylabel("Количество объектов")
                ax.legend()
                auto_title = f"Распределение {resolved_field} — {display_name}"

            elif chart_type == "bar":
                col_name = group_by or resolved_field
                if col_name not in gdf.columns:
                    col_name = resolved_field
                counts = gdf[col_name].dropna().astype(str).value_counts().head(20)
                counts.plot(kind="barh", ax=ax, color="steelblue", edgecolor="white")
                ax.set_xlabel("Количество объектов")
                ax.invert_yaxis()
                auto_title = f"Топ значений: {col_name} — {display_name}"

            elif chart_type == "pie":
                col_name = group_by or resolved_field
                if col_name not in gdf.columns:
                    col_name = resolved_field
                counts = gdf[col_name].dropna().astype(str).value_counts().head(12)
                wedges, texts, autotexts = ax.pie(
                    counts.values, labels=counts.index, autopct="%1.1f%%",
                    startangle=90, pctdistance=0.85,
                )
                for t in autotexts:
                    t.set_fontsize(8)
                ax.set_aspect("equal")
                auto_title = f"Распределение: {col_name} — {display_name}"

            elif chart_type == "scatter":
                if not field2:
                    return json.dumps({"error": "Для scatter необходим field2 (поле для оси Y)."}, ensure_ascii=False)
                if field2 not in gdf.columns:
                    return json.dumps({"error": f"Поле '{field2}' не найдено."}, ensure_ascii=False)
                x_vals = gdf[resolved_field].dropna()
                common_idx = x_vals.index.intersection(gdf[field2].dropna().index)
                x_vals = gdf.loc[common_idx, resolved_field]
                y_vals = gdf.loc[common_idx, field2]
                ax.scatter(x_vals, y_vals, s=4, alpha=0.4, color="steelblue")
                corr = x_vals.corr(y_vals)
                ax.set_xlabel(resolved_field + (f" ({units})" if units else ""))
                ax.set_ylabel(field2)
                ax.text(0.05, 0.95, f"r = {corr:.3f}", transform=ax.transAxes,
                        fontsize=11, va="top",
                        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.6))
                ax.axhline(0, color="gray", linestyle="--", linewidth=0.5)
                ax.axvline(0, color="gray", linestyle="--", linewidth=0.5)
                ax.grid(True, alpha=0.3)
                auto_title = f"Корреляция: {resolved_field} vs {field2} — {display_name}"

            elif chart_type == "profile":
                axis = (field2 or "lon").lower()
                if axis not in ("lat", "lon"):
                    return json.dumps({"error": "field2 для profile должен быть 'lat' или 'lon'."}, ensure_ascii=False)
                if gdf.geometry is None:
                    return json.dumps({"error": "Слой не имеет геометрии — profile недоступен."}, ensure_ascii=False)
                coord = gdf.geometry.y if axis == "lat" else gdf.geometry.x
                vals = gdf[resolved_field]
                valid = coord.notna() & vals.notna()
                coord_v = coord[valid].values
                vals_v = vals[valid].values
                sort_idx = np.argsort(coord_v)
                ax.plot(coord_v[sort_idx], vals_v[sort_idx], linewidth=1, color="steelblue")
                ax.fill_between(coord_v[sort_idx], vals_v[sort_idx], alpha=0.2, color="steelblue")
                ax.axhline(0, color="gray", linestyle="--", linewidth=0.5)
                ax.grid(True, alpha=0.3)
                ax.set_xlabel("Широта, °N" if axis == "lat" else "Долгота, °E")
                ax.set_ylabel(resolved_field + (f" ({units})" if units else ""))
                auto_title = f"Профиль {resolved_field} по {'широте' if axis == 'lat' else 'долготе'} — {display_name}"

            else:
                return json.dumps({"error": f"Неизвестный chart_type: '{chart_type}'. Допустимые: histogram, bar, pie, scatter, profile."}, ensure_ascii=False)

        except Exception as e:
            plt.close(fig)
            return json.dumps({"error": f"Ошибка построения графика: {e}"}, ensure_ascii=False)

        ax.set_title(title or auto_title, fontsize=12, fontweight="bold")
        plt.tight_layout()

        slug = f"{layer_id}_{chart_type}"
        out_path = _save_figure(fig, _out_dir(pid, output_dir), slug)

        return json.dumps({
            "file": out_path,
            "layer": layer_id,
            "display_name": display_name,
            "chart_type": chart_type,
            "field": resolved_field,
            "field2": field2,
            "features_used": int(len(gdf)),
            "truncated": truncated,
        }, ensure_ascii=False, indent=2)

    # -----------------------------------------------------------------------
    # interpolate_field
    # -----------------------------------------------------------------------

    def interpolate_field(
        layer: str,
        value_field: str,
        method: str = "linear",
        colormap: str = "RdYlBu_r",
        grid_resolution: int = 300,
        overlay_layer: str | None = None,
        title: str | None = None,
        output_dir: str | None = None,
        project_id: str | None = None,
    ) -> str:
        """Интерполировать числовое поле точечного слоя в растровую сетку и сохранить PNG.

        Типичное применение: карты гравитационного/магнитного поля и их градиентов.
        Использует scipy.interpolate.griddata для интерполяции нерегулярной сетки
        на равномерную и визуализирует результат через pcolormesh.

        Args:
            layer: Точечный слой (display_name, layer_id или alias).
            value_field: Числовое поле для интерполяции.
                         Пример: "ID_123" (ΔG), "дельта_T" (ΔT).
            method: Метод scipy.griddata: "linear" (по умолчанию), "nearest", "cubic".
                    "linear" — оптимально для геофизических полей.
                    "cubic"  — более гладко, медленнее, возможны выбросы.
            colormap: Matplotlib colormap.
                      Рекомендации: "RdYlBu_r" (поля ΔG), "RdBu_r" (ΔT), "hot_r" (градиенты).
            grid_resolution: Число ячеек по каждой оси (по умолчанию 300×300).
            overlay_layer: Опциональный векторный оверлей поверх растра.
                           Например: лицензионный контур, тектоника.
            title: Заголовок карты.
            output_dir: Директория для сохранения PNG.
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            from scipy.interpolate import griddata as scipy_griddata
        except ImportError:
            return json.dumps({"error": "scipy не установлен. Добавьте scipy>=1.12 в requirements.txt."}, ensure_ascii=False)

        try:
            pid = _resolve_project(project_id)
            layer_id = _resolve_layer(pid, layer)
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
            entry = store.get_layer_entry(manifest, layer_id) or {}
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            gdf = gpd.read_file(gdb_path, layer=layer_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя: {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{layer_id}' пустой."}, ensure_ascii=False)

        if value_field not in gdf.columns:
            available = [c for c in gdf.columns if c != "geometry"]
            return json.dumps({
                "error": f"Поле '{value_field}' не найдено.",
                "available_fields": available,
            }, ensure_ascii=False)

        # Координаты и значения
        try:
            lons = gdf.geometry.x.values.astype(float)
            lats = gdf.geometry.y.values.astype(float)
            vals = gdf[value_field].values.astype(float)
        except Exception as e:
            return json.dumps({"error": f"Ошибка преобразования координат/значений: {e}"}, ensure_ascii=False)

        valid_mask = np.isfinite(lons) & np.isfinite(lats) & np.isfinite(vals)
        lons, lats, vals = lons[valid_mask], lats[valid_mask], vals[valid_mask]

        if len(vals) < 4:
            return json.dumps({"error": "Слишком мало валидных точек для интерполяции (< 4)."}, ensure_ascii=False)

        # Экстент с 10% отступом
        dx = (lons.max() - lons.min()) * 0.1 or 0.01
        dy = (lats.max() - lats.min()) * 0.1 or 0.01
        x0, x1 = lons.min() - dx, lons.max() + dx
        y0, y1 = lats.min() - dy, lats.max() + dy

        grid_x = np.linspace(x0, x1, grid_resolution)
        grid_y = np.linspace(y0, y1, grid_resolution)
        gxx, gyy = np.meshgrid(grid_x, grid_y)

        try:
            grid_z = scipy_griddata((lons, lats), vals, (gxx, gyy), method=method)
        except Exception as e:
            return json.dumps({"error": f"Ошибка интерполяции: {e}"}, ensure_ascii=False)

        vmin = float(np.nanpercentile(vals, 2))
        vmax = float(np.nanpercentile(vals, 98))

        fig, ax = plt.subplots(figsize=(12, 10))

        im = ax.pcolormesh(gxx, gyy, grid_z, cmap=colormap, vmin=vmin, vmax=vmax, shading="auto")
        units = entry.get("units", "")
        plt.colorbar(im, ax=ax, label=f"{value_field}" + (f" ({units})" if units else ""), shrink=0.8)

        # Оверлей
        overlay_id = None
        if overlay_layer:
            try:
                overlay_id = _resolve_layer(pid, overlay_layer)
                ol_gdf = gpd.read_file(gdb_path, layer=overlay_id)
                _draw_layer(ax, ol_gdf, color_by=None, color="white", linewidth=1.5, alpha=0.9)
            except Exception:
                overlay_id = None

        display_name = entry.get("display_name", layer_id)
        auto_title = f"Интерполяция {value_field} — {display_name}"

        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_xlabel("Долгота, °E")
        ax.set_ylabel("Широта, °N")
        ax.set_title(title or auto_title, fontsize=13, fontweight="bold")
        ax.set_aspect("equal")
        plt.tight_layout()

        slug = f"{layer_id}_interp_{value_field}"
        out_path = _save_figure(fig, _out_dir(pid, output_dir), slug)

        return json.dumps({
            "file": out_path,
            "layer": layer_id,
            "display_name": display_name,
            "value_field": value_field,
            "method": method,
            "grid_resolution": grid_resolution,
            "points_used": int(len(vals)),
            "value_range": {"min": round(float(vals.min()), 6), "max": round(float(vals.max()), 6),
                             "mean": round(float(vals.mean()), 6)},
            "overlay": overlay_id,
        }, ensure_ascii=False, indent=2)

    return [visualize_layer, plot_statistics, interpolate_field]
