"""P1 инструменты запросов к .gdb — query_features, summarize_layer.

Используют geopandas/fiona для прямого чтения файловой геобазы.
Вызываются только когда manifest не содержит достаточно информации.
"""

from __future__ import annotations

import json
from typing import Any, Callable

import fiona
import geopandas as gpd
import numpy as np

from ..project_store import ProjectStore


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _safe_val(v: Any) -> Any:
    """Привести значение к JSON-сериализуемому типу."""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def _row_to_dict(row: dict) -> dict:
    return {k: _safe_val(v) for k, v in row.items() if k != "geometry"}


def _build_filter_mask(gdf: gpd.GeoDataFrame, filters: dict) -> Any:
    """Построить маску фильтрации по словарю {field: value}.

    Поддерживаемые операторы внутри value:
        ">=5.0"  — числовое сравнение
        "<=10"
        "2010"   — числовое равенство или строковое вхождение
    """
    mask = np.ones(len(gdf), dtype=bool)
    for field, raw_value in filters.items():
        if field not in gdf.columns:
            continue
        col = gdf[field]
        sval = str(raw_value).strip()

        # Числовые операторы
        if sval.startswith(">="):
            try:
                mask &= col >= float(sval[2:])
            except (ValueError, TypeError):
                pass
        elif sval.startswith("<="):
            try:
                mask &= col <= float(sval[2:])
            except (ValueError, TypeError):
                pass
        elif sval.startswith(">"):
            try:
                mask &= col > float(sval[1:])
            except (ValueError, TypeError):
                pass
        elif sval.startswith("<"):
            try:
                mask &= col < float(sval[1:])
            except (ValueError, TypeError):
                pass
        else:
            # Точное равенство или строковое вхождение (case-insensitive)
            try:
                num_val = float(sval)
                mask &= col == num_val
            except ValueError:
                mask &= col.astype(str).str.contains(sval, case=False, na=False)
    return mask


# ---------------------------------------------------------------------------
# make_tools
# ---------------------------------------------------------------------------

def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите list_projects() и get_project_summary()."
            )
        return pid

    def _resolve_layer(pid: str, layer: str) -> tuple[str, dict]:
        """Вернуть (layer_id, layer_entry) или выбросить ValueError."""
        layer_id = store.resolve_layer_name(pid, layer)
        if layer_id is None:
            raise ValueError(
                f"Слой '{layer}' не найден. Используй list_layers() чтобы увидеть доступные слои."
            )
        manifest = store.get_manifest(pid)
        entry = store.get_layer_entry(manifest, layer_id)
        return layer_id, entry or {}

    # -------------------------------------------------------------------
    # query_features
    # -------------------------------------------------------------------
    def query_features(
        layer: str,
        filters: str | None = None,
        limit: int = 50,
        fields: str | None = None,
        project_id: str | None = None,
    ) -> str:
        """Получить объекты из слоя с фильтрацией по атрибутам.

        Читает напрямую из .gdb. Используй для получения конкретных значений
        или когда нужно больше деталей, чем есть в describe_layer().

        Args:
            layer: Название слоя (display_name, layer_id или alias).
            filters: JSON-объект с условиями фильтрации.
                     Пример: '{"vid_iz": "Геологическая съёмка", "scale": "1:200000"}'
                     Операторы: ">=2010", "<=100", "Слово" (вхождение).
            limit: Максимальное количество объектов в ответе (по умолчанию 50, макс 500).
            fields: Через запятую — какие поля вернуть.
                    Пример: "Имя,Участ,POINT_X,POINT_Y".
                    Если не указано — все поля.
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            layer_id, layer_entry = _resolve_layer(pid, layer)
            gdb_path = store.get_gdb_path(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        limit = min(max(1, limit), 500)

        # Предупреждение для больших слоёв
        feature_count = layer_entry.get("feature_count", 0)
        warning = None
        if feature_count > 10_000:
            warning = (
                f"Слой содержит {feature_count:,} объектов. "
                f"Возвращается максимум {limit}. Используй фильтры для уточнения."
            )

        try:
            gdf = gpd.read_file(gdb_path, layer=layer_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения .gdb: {e}"}, ensure_ascii=False)

        # Применяем фильтры
        filters_dict = {}
        if filters:
            try:
                filters_dict = json.loads(filters)
            except json.JSONDecodeError:
                return json.dumps({"error": f"filters: невалидный JSON: {filters}"}, ensure_ascii=False)

        if filters_dict:
            mask = _build_filter_mask(gdf, filters_dict)
            gdf = gdf[mask]

        total_after_filter = len(gdf)
        gdf = gdf.head(limit)

        # Выбор полей
        if fields:
            selected = [f.strip() for f in fields.split(",") if f.strip() in gdf.columns]
            if selected:
                gdf = gdf[selected]

        # Сериализация без геометрии
        rows = [_row_to_dict(row) for row in gdf.drop(columns=["geometry"], errors="ignore").to_dict("records")]

        result: dict = {
            "layer": layer_entry.get("display_name", layer_id),
            "layer_id": layer_id,
            "total_after_filter": total_after_filter,
            "returned": len(rows),
            "features": rows,
        }
        if warning:
            result["warning"] = warning
        if filters_dict:
            result["applied_filters"] = filters_dict

        return json.dumps(result, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------------
    # summarize_layer
    # -------------------------------------------------------------------
    def summarize_layer(
        layer: str,
        project_id: str | None = None,
    ) -> str:
        """Вычислить актуальную статистику по полям слоя из .gdb.

        Используй когда describe_layer() не имеет статистики
        (слой большой, >10k объектов) или нужны свежие данные.

        Возвращает для числовых полей: min, max, mean с единицами измерения.
        Для строковых: количество уникальных значений и топ-20.

        Args:
            layer: Название слоя (display_name, layer_id или alias).
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            layer_id, layer_entry = _resolve_layer(pid, layer)
            gdb_path = store.get_gdb_path(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        feature_count = layer_entry.get("feature_count", 0)

        try:
            gdf = gpd.read_file(gdb_path, layer=layer_id)
        except Exception as e:
            return json.dumps({"error": f"Ошибка чтения .gdb: {e}"}, ensure_ascii=False)

        units = layer_entry.get("units")
        stats: list[dict] = []

        for col in gdf.columns:
            if col == "geometry":
                continue
            series = gdf[col]
            nulls = int(series.isna().sum())
            entry: dict = {"field": col, "nulls": nulls}

            if np.issubdtype(series.dtype, np.number):
                valid = series.dropna()
                if len(valid) > 0:
                    suffix = f" {units}" if units else ""
                    entry["type"] = "numeric"
                    entry["min"] = f"{float(valid.min()):.6g}{suffix}"
                    entry["max"] = f"{float(valid.max()):.6g}{suffix}"
                    entry["mean"] = f"{float(valid.mean()):.6g}{suffix}"
                    entry["std"] = f"{float(valid.std()):.4g}"
            else:
                valid = series.dropna().astype(str)
                vc = valid.value_counts().head(20)
                entry["type"] = "categorical"
                entry["unique_count"] = int(valid.nunique())
                entry["top_values"] = {str(k): int(v) for k, v in vc.items()}

            stats.append(entry)

        return json.dumps({
            "layer": layer_entry.get("display_name", layer_id),
            "layer_id": layer_id,
            "feature_count": feature_count,
            "units": units,
            "fields_stats": stats,
        }, ensure_ascii=False, indent=2)

    return [query_features, summarize_layer]
