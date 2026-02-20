"""P1 инструмент поиска по изученности — search_izuchennost.

Ищет в слоях типа "Изученность" (Izuch_A_sel и подобных) записи о
ранее выполненных геологических работах.
"""

from __future__ import annotations

import json
from typing import Callable

import geopandas as gpd

from ..project_store import ProjectStore


# Известные имена слоёв изученности (dataset_name или display_name)
_IZUCH_LAYER_PATTERNS = [
    "izuch", "изученн", "survey", "работ", "opmar",
]


def _find_izuchennost_layers(manifest: dict) -> list[str]:
    """Найти layer_id слоёв изученности в manifest."""
    found = []
    for layer in manifest.get("layers", []):
        layer_id = layer["layer_id"].lower()
        display = layer.get("display_name", "").lower()
        group = (layer.get("group") or "").lower()

        if any(p in layer_id or p in display or p in group for p in _IZUCH_LAYER_PATTERNS):
            # Исключаем таблицы вложений — они не содержат атрибутов изученности
            if "__attach" not in layer_id:
                found.append(layer["layer_id"])
    return found


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите get_project_summary(project_id=...)."
            )
        return pid

    def search_izuchennost(
        query: str | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        work_type: str | None = None,
        scale: str | None = None,
        limit: int = 30,
        project_id: str | None = None,
    ) -> str:
        """Поиск ранее выполненных геологических работ по территории.

        Ищет в слоях изученности (Izuch_A_sel и подобных) по:
        - типу работ (геологическая съёмка, аэромагнитная и т.д.)
        - годам проведения
        - масштабу
        - ключевым словам (название отчёта, авторы, организация)

        Используй для вопросов "Какие работы проводились на этой территории?",
        "Есть ли аэромагнитные данные после 2000 года?", и т.д.

        Args:
            query: Текстовый поиск по названию отчёта, авторам, организации.
                   Пример: "аэромагнитная", "Лукойл", "Ухтагеофизика"
            year_from: Год начала работ не раньше (включительно).
            year_to: Год окончания работ не позже (включительно).
            work_type: Вид работ (частичное совпадение).
                       Примеры: "Геологическая съёмка", "Аэромагнитная", "Геохимическая"
            scale: Масштаб работ (частичное совпадение).
                   Примеры: "1:200000", "1:50000"
            limit: Максимум записей в ответе (по умолчанию 30, макс 200).
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            manifest = store.get_manifest(pid)
            gdb_path = store.get_gdb_path(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        limit = min(max(1, limit), 200)

        # Найти слои изученности
        izuch_layer_ids = _find_izuchennost_layers(manifest)
        if not izuch_layer_ids:
            return json.dumps({
                "error": "Слои изученности не найдены в проекте.",
                "hint": "Проверьте list_layers() — ищите слои с группой 'Изученность'."
            }, ensure_ascii=False)

        all_results = []
        layers_searched = []

        for layer_id in izuch_layer_ids:
            try:
                gdf = gpd.read_file(gdb_path, layer=layer_id)
            except Exception as e:
                continue

            layers_searched.append(layer_id)
            cols_lower = {c.lower(): c for c in gdf.columns}

            # --- Определяем имена полей ---
            def col(name: str) -> str | None:
                return cols_lower.get(name)

            field_vid  = col("vid_iz") or col("type") or col("vid")       # вид работ
            field_ynach = col("god_nach") or col("year_from") or col("g_nach")  # год начала
            field_yend  = col("god_end") or col("year_to") or col("g_end")      # год конца
            field_scale = col("scale") or col("masshtab")
            field_name  = col("name_otch") or col("name") or col("otchet")      # название
            field_auth  = col("avts") or col("authors") or col("avtor")         # авторы
            field_org   = col("org_isp") or col("org") or col("organization")   # организация
            field_method = col("method")

            # --- Текстовые поля для общего поиска ---
            text_fields = [f for f in [field_name, field_auth, field_org, field_method, field_vid]
                           if f is not None]

            mask = [True] * len(gdf)
            mask_series = gdf.index.map(lambda _: True)
            import pandas as pd
            mask_series = pd.Series([True] * len(gdf), index=gdf.index)

            # Текстовый поиск
            if query:
                q_lower = query.lower()
                text_mask = pd.Series([False] * len(gdf), index=gdf.index)
                for tf in text_fields:
                    text_mask |= gdf[tf].astype(str).str.lower().str.contains(q_lower, na=False)
                mask_series &= text_mask

            # Фильтр по виду работ
            if work_type and field_vid:
                mask_series &= gdf[field_vid].astype(str).str.lower().str.contains(
                    work_type.lower(), na=False
                )

            # Фильтр по масштабу
            if scale and field_scale:
                mask_series &= gdf[field_scale].astype(str).str.contains(scale, na=False)

            # Фильтры по годам
            if year_from is not None and field_ynach:
                try:
                    year_col = pd.to_numeric(gdf[field_ynach], errors="coerce")
                    mask_series &= (year_col >= year_from) | year_col.isna()
                    mask_series &= ~year_col.isna()
                    mask_series &= year_col >= year_from
                except Exception:
                    pass

            if year_to is not None and field_yend:
                try:
                    year_col = pd.to_numeric(gdf[field_yend], errors="coerce")
                    mask_series &= (year_col <= year_to) | year_col.isna()
                    mask_series &= ~year_col.isna()
                    mask_series &= year_col <= year_to
                except Exception:
                    pass

            filtered = gdf[mask_series]

            for _, row in filtered.iterrows():
                record: dict = {"source_layer": layer_id}
                if field_vid:
                    record["вид_работ"] = str(row.get(field_vid, ""))
                if field_name:
                    record["название_отчёта"] = str(row.get(field_name, ""))
                if field_auth:
                    record["авторы"] = str(row.get(field_auth, ""))
                if field_org:
                    record["организация"] = str(row.get(field_org, ""))
                if field_ynach:
                    record["год_начала"] = str(row.get(field_ynach, ""))
                if field_yend:
                    record["год_окончания"] = str(row.get(field_yend, ""))
                if field_scale:
                    record["масштаб"] = str(row.get(field_scale, ""))
                if field_method:
                    record["метод"] = str(row.get(field_method, ""))
                # Глобальный id для последующего extract_attachment
                gid = row.get("GlobalID") or row.get("OBJECTID") or row.get("FID")
                if gid is not None:
                    record["id"] = str(gid)
                all_results.append(record)

        total = len(all_results)
        truncated = all_results[:limit]

        return json.dumps({
            "total_found": total,
            "returned": len(truncated),
            "layers_searched": layers_searched,
            "filters": {
                k: v for k, v in {
                    "query": query,
                    "year_from": year_from,
                    "year_to": year_to,
                    "work_type": work_type,
                    "scale": scale,
                }.items() if v is not None
            },
            "results": truncated,
            "hint": (
                "Для извлечения PDF-карточек используй: "
                "list_attachments(layer='Izuch_A_sel') и extract_attachment()"
            ) if total > 0 else None,
        }, ensure_ascii=False, indent=2)

    return [search_izuchennost]
