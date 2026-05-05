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
        if not project_id:
            raise ValueError("project_id обязателен.")
        return project_id

    def _load_all_records(pid: str) -> tuple[list[dict], list[str]]:
        """Загрузить все записи из всех слоёв изученности. Возвращает (records, layers_searched)."""
        manifest = store.get_manifest(pid)
        gdb_path = store.get_gdb_path(pid)
        izuch_layer_ids = _find_izuchennost_layers(manifest)
        if not izuch_layer_ids:
            return [], []

        all_records: list[dict] = []
        layers_searched: list[str] = []

        for layer_id in izuch_layer_ids:
            try:
                gdf = gpd.read_file(gdb_path, layer=layer_id)
            except Exception:
                continue

            layers_searched.append(layer_id)
            cols_lower = {c.lower(): c for c in gdf.columns}

            def col(name: str) -> str | None:
                return cols_lower.get(name)

            field_vid    = col("vid_iz") or col("type") or col("vid")
            field_ynach  = col("god_nach") or col("year_from") or col("g_nach")
            field_yend   = col("god_end") or col("year_to") or col("g_end")
            field_scale  = col("scale") or col("masshtab")
            field_name   = col("name_otch") or col("name") or col("otchet")
            field_auth   = col("avts") or col("authors") or col("avtor")
            field_org    = col("org_isp") or col("org") or col("organization")
            field_method = col("method")

            for _, row in gdf.iterrows():
                r: dict = {"source_layer": layer_id}
                if field_vid:    r["вид_работ"]       = str(row.get(field_vid, "") or "")
                if field_name:   r["название_отчёта"] = str(row.get(field_name, "") or "")
                if field_auth:   r["авторы"]          = str(row.get(field_auth, "") or "")
                if field_org:    r["организация"]     = str(row.get(field_org, "") or "")
                if field_method: r["метод"]           = str(row.get(field_method, "") or "")
                if field_scale:  r["масштаб"]         = str(row.get(field_scale, "") or "")
                if field_ynach:  r["год_начала"]      = str(row.get(field_ynach, "") or "")
                if field_yend:   r["год_окончания"]   = str(row.get(field_yend, "") or "")
                gid = row.get("GlobalID") or row.get("OBJECTID") or row.get("FID")
                if gid is not None:
                    r["id"] = str(gid)
                all_records.append(r)

        return all_records, layers_searched

    def search_izuchennost(
        project_id: str | None = None,
    ) -> str:
        """Сводка ранее выполненных геологических работ, сгруппированная по десятилетиям.

        Показывает: сколько работ в каждом десятилетии, какие организации, методы и масштабы.
        Используй для первичного обзора изученности территории.
        Для получения конкретных записей используй get_izuchennost_records().
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            records, layers_searched = _load_all_records(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        if not layers_searched:
            return json.dumps({
                "error": "Слои изученности не найдены в проекте.",
                "hint": "Проверьте list_layers() — ищите слои с группой 'Изученность'."
            }, ensure_ascii=False)

        decades: dict[str, dict] = {}
        no_year = 0
        for r in records:
            try:
                y = int(float(r.get("год_начала", "")))
                decade = f"{(y // 10) * 10}–{(y // 10) * 10 + 9}"
            except (ValueError, TypeError):
                no_year += 1
                continue

            d = decades.setdefault(decade, {
                "count": 0,
                "work_types": set(),
                "organizations": set(),
                "methods": set(),
                "scales": set(),
            })
            d["count"] += 1
            if r.get("вид_работ"):    d["work_types"].add(r["вид_работ"])
            if r.get("организация"):  d["organizations"].add(r["организация"])
            if r.get("метод"):        d["methods"].add(r["метод"])
            if r.get("масштаб"):      d["scales"].add(r["масштаб"])

        by_decade = {
            k: {
                "count":         v["count"],
                "work_types":    sorted(v["work_types"]),
                "organizations": sorted(v["organizations"]),
                "methods":       sorted(v["methods"]),
                "scales":        sorted(v["scales"]),
            }
            for k, v in sorted(decades.items())
        }

        return json.dumps({
            "total_records": len(records),
            "layers_searched": layers_searched,
            "no_year_count": no_year,
            "by_decade": by_decade,
            "hint": "Для полных записей используй get_izuchennost_records(year_from=..., year_to=..., method=..., organization=...)",
        }, ensure_ascii=False, indent=2)

    def get_izuchennost_records(
        year_from: int | None = None,
        year_to: int | None = None,
        method: str | None = None,
        organization: str | None = None,
        limit: int = 50,
        project_id: str | None = None,
    ) -> str:
        """Полные записи об отдельных геологических работах с фильтрацией.

        Args:
            year_from: Год начала работ не раньше (включительно).
            year_to: Год окончания работ не позже (включительно).
            method: Метод работ (частичное совпадение). Пример: "ГДП", "РНГ", "ТЕМ-Ц".
            organization: Название организации (частичное совпадение). Пример: "Севморгео".
            limit: Максимум записей в ответе (по умолчанию 50, макс 200).
            project_id: ID проекта (необязательно, если уже выбран).

        Возвращает полные записи: source_layer, вид_работ, название_отчёта, авторы,
        организация, год_начала, год_окончания, масштаб, метод, id.
        id используй в extract_attachment() для получения PDF-карточки.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            records, layers_searched = _load_all_records(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        if not layers_searched:
            return json.dumps({"error": "Слои изученности не найдены в проекте."}, ensure_ascii=False)

        limit = min(max(1, limit), 200)
        filtered = records

        if year_from is not None:
            def _yfrom(r: dict) -> bool:
                try:
                    return int(float(r.get("год_начала", ""))) >= year_from
                except (ValueError, TypeError):
                    return False
            filtered = [r for r in filtered if _yfrom(r)]

        if year_to is not None:
            def _yto(r: dict) -> bool:
                try:
                    return int(float(r.get("год_окончания", "") or r.get("год_начала", ""))) <= year_to
                except (ValueError, TypeError):
                    return False
            filtered = [r for r in filtered if _yto(r)]

        if method:
            m_lower = method.lower()
            filtered = [r for r in filtered if m_lower in r.get("метод", "").lower()]

        if organization:
            org_lower = organization.lower()
            filtered = [r for r in filtered if org_lower in r.get("организация", "").lower()]

        total = len(filtered)
        return json.dumps({
            "total_found": total,
            "returned": min(total, limit),
            "layers_searched": layers_searched,
            "filters": {k: v for k, v in {
                "year_from": year_from,
                "year_to": year_to,
                "method": method,
                "organization": organization,
            }.items() if v is not None},
            "results": filtered[:limit],
        }, ensure_ascii=False, indent=2)

    return [search_izuchennost, get_izuchennost_records]
