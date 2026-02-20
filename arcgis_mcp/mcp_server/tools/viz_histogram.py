"""Tool: plot_histogram — статистическая визуализация атрибутов слоя (matplotlib).

Автоматически выбирает тип графика по dtype и числу уникальных значений:
  - histogram: числовые поля с многими уникальными значениями
  - bar / bar_top20: категориальные или числовые с малым числом уникальных значений
  - boxplot: числовые, сгруппированные по полю group_by
"""

from __future__ import annotations

import json
import time
from typing import Callable

import matplotlib.pyplot as plt
import numpy as np

from ..project_store import ProjectStore
from .viz_utils import (
    load_and_reproject,
    field_stats,
    save_figure,
)

_SKIP_FIELDS = {"objectid", "fid", "shape_length", "shape_area", "globalid", "shape"}


def _auto_plot_type(dtype: str, unique_count: int, total_count: int) -> str:
    """Выбрать тип графика по характеристикам поля."""
    if dtype in ("float64", "float32", "int64", "int32"):
        if unique_count < 15:
            return "bar"
        return "histogram"
    elif dtype in ("object", "string", "category"):
        if unique_count <= 30:
            return "bar"
        return "bar_top20"
    return "histogram"


def _pick_field(gdf, preferred: str | None) -> str | None:
    """Выбрать поле: предпочтённое → первое числовое → первое строковое."""
    if preferred and preferred in gdf.columns:
        return preferred
    cols = [c for c in gdf.columns if c.lower() not in _SKIP_FIELDS and c != "geometry"]
    for c in cols:
        if np.issubdtype(gdf[c].dtype, np.number):
            return c
    return cols[0] if cols else None


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError("Проект не выбран. Вызовите get_project_summary(project_id=...).")
        return pid

    def plot_histogram(
        layer_id: str,
        field: str,
        project_id: str | None = None,
        plot_type: str = "auto",
        group_by: str | None = None,
        bins: int = 50,
        title: str | None = None,
        output_format: str = "png",
    ) -> str:
        """Построить статистический график по полю слоя и сохранить PNG.

        Автоматически выбирает тип графика: гистограмма для числовых полей,
        bar-chart для категориальных, boxplot для группировки.
        Возвращает путь к файлу и базовую статистику поля.

        Args:
            layer_id: ID или display_name слоя из manifest.
            field: Имя поля для анализа.
            project_id: ID проекта (необязательно, если уже выбран).
            plot_type: Тип графика:
                "auto"      — автоматически по dtype и числу уникальных значений.
                "histogram" — гистограмма числового поля (+ линии mean/median).
                "bar"       — горизонтальный барчарт топ-значений.
                "bar_top20" — top-20 значений (для категорий с >30 уникальных).
                "boxplot"   — box-plot по группам (требует group_by).
            group_by: Поле группировки (для boxplot и bar).
                      Пример: "Участ" — группировать скважины по участкам.
            bins: Количество бинов для гистограммы (по умолчанию 50).
            title: Заголовок. None → автогенерация.
            output_format: "png" или "svg".
        """
        try:
            pid = _resolve_project(project_id)
            resolved_id = store.resolve_layer_name(pid, layer_id) or layer_id
            gdb_path = store.get_gdb_path(pid)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        entry = store.get_layer_entry(manifest, resolved_id) or {}
        display_name = entry.get("display_name", resolved_id)
        units = entry.get("units")

        try:
            gdf = load_and_reproject(gdb_path, resolved_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения слоя '{resolved_id}': {e}"}, ensure_ascii=False)

        if gdf.empty:
            return json.dumps({"error": f"Слой '{resolved_id}' пустой."}, ensure_ascii=False)

        # Разрешить field (с нечётким поиском)
        resolved_field = _pick_field(gdf, field)
        if resolved_field is None:
            return json.dumps({"error": "Нет подходящих полей для анализа."}, ensure_ascii=False)

        if field and field not in gdf.columns:
            # fuzzy
            fl = field.lower()
            for col in gdf.columns:
                if col.lower() == fl:
                    resolved_field = col
                    break

        series = gdf[resolved_field].dropna()
        dtype_str = str(gdf[resolved_field].dtype)
        unique_count = int(series.nunique())

        # Авто-тип
        resolved_type = plot_type
        if plot_type == "auto":
            resolved_type = _auto_plot_type(dtype_str, unique_count, len(gdf))
        if plot_type == "boxplot" and not group_by:
            resolved_type = "histogram"  # fallback

        stats = field_stats(gdf[resolved_field])
        units_suffix = f" ({units})" if units else ""
        auto_title = title or f"{display_name} — {resolved_field}{units_suffix}"

        fig, ax = plt.subplots(figsize=(12, 6))

        try:
            if resolved_type == "histogram":
                is_numeric = np.issubdtype(gdf[resolved_field].dtype, np.number)
                if not is_numeric:
                    resolved_type = "bar"
                else:
                    ax.hist(series, bins=bins, color="steelblue", edgecolor="white", alpha=0.85)
                    mean_val = series.mean()
                    median_val = series.median()
                    ax.axvline(mean_val, color="red", linestyle="--",
                               label=f"Среднее: {mean_val:.4g}")
                    ax.axvline(median_val, color="orange", linestyle="--",
                               label=f"Медиана: {median_val:.4g}")
                    ax.set_xlabel(f"{resolved_field}{units_suffix}")
                    ax.set_ylabel("Количество объектов")
                    ax.legend(fontsize=9)

            if resolved_type in ("bar", "bar_top20"):
                n = 20 if resolved_type == "bar_top20" else min(unique_count, 30)
                if group_by and group_by in gdf.columns:
                    # Сгруппированный подсчёт
                    counts = gdf.groupby(group_by)[resolved_field].count().sort_values(ascending=False).head(n)
                    x_label = f"Кол-во {resolved_field} по {group_by}"
                else:
                    counts = series.astype(str).value_counts().head(n)
                    x_label = "Количество объектов"

                colors = plt.cm.get_cmap("tab20", len(counts))(range(len(counts)))
                counts.plot(kind="barh", ax=ax, color=colors, edgecolor="white")
                ax.set_xlabel(x_label)
                ax.invert_yaxis()

            elif resolved_type == "boxplot" and group_by and group_by in gdf.columns:
                if not np.issubdtype(gdf[resolved_field].dtype, np.number):
                    return json.dumps(
                        {"error": f"Boxplot требует числового поля, '{resolved_field}' не числовое."},
                        ensure_ascii=False,
                    )
                groups = gdf.groupby(group_by)[resolved_field].apply(list)
                ax.boxplot(groups.values, labels=groups.index, vert=True)
                ax.set_xlabel(group_by)
                ax.set_ylabel(f"{resolved_field}{units_suffix}")
                ax.tick_params(axis="x", rotation=45)

        except Exception as e:
            plt.close(fig)
            return json.dumps({"error": f"Ошибка построения графика: {e}"}, ensure_ascii=False)

        ax.set_title(auto_title, fontsize=13, fontweight="bold")
        plt.tight_layout()

        safe_name = f"{resolved_id}_{resolved_field}_{resolved_type}_{int(time.time())}"
        out_path = save_figure(fig, pid, safe_name.replace("/", "_"), fmt=output_format)

        return json.dumps({
            "file": out_path,
            "layer": resolved_id,
            "display_name": display_name,
            "field": resolved_field,
            "plot_type": resolved_type,
            "feature_count": len(gdf),
            "field_stats": stats,
        }, ensure_ascii=False, indent=2)

    return [plot_histogram]
