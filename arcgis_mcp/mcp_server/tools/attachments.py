"""P1 инструменты работы с вложениями — list_attachments, extract_attachment.

Вложения хранятся в таблицах *__ATTACH в .gdb.
Могут быть извлечены на диск (PDF, изображения и т.д.).
"""

from __future__ import annotations

import json
from typing import Callable

import fiona

from ..project_store import ProjectStore


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        if not project_id:
            raise ValueError("project_id обязателен.")
        return project_id

    def list_attachments(
        layer: str | None = None,
        project_id: str | None = None,
    ) -> str:
        """Показать список файлов-вложений (PDF, изображения) проекта.

        Вложения хранятся в таблицах *__ATTACH в геобазе и связаны
        с объектами через REL_GLOBALID.

        Args:
            layer: Имя родительского слоя (например "Izuch_A_sel", "изученность").
                   Если не указано — показать все вложения всех слоёв.
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        attachments_summary = manifest.get("attachments_summary", {})
        if not attachments_summary.get("tables"):
            return json.dumps({
                "message": "В проекте нет таблиц вложений.",
                "project": pid,
            }, ensure_ascii=False)

        # Определяем какие таблицы смотреть
        target_tables: list[str] = []
        if layer:
            layer_id = store.resolve_layer_name(pid, layer)
            if layer_id:
                target_tables = [f"{layer_id}__ATTACH"]
            else:
                # Ищем прямо по имени таблицы
                for t in attachments_summary.get("tables", []):
                    if layer.lower() in t.lower():
                        target_tables.append(t)
        else:
            target_tables = attachments_summary.get("tables", [])

        if not target_tables:
            return json.dumps({
                "error": f"Таблица вложений для слоя '{layer}' не найдена.",
                "available_tables": attachments_summary.get("tables", []),
            }, ensure_ascii=False)

        # Читаем из manifest (если уже есть кэш в layer_profiles)
        stats_by_table: dict = {}
        total_count = 0
        total_size = 0
        type_counts: dict[str, int] = {}

        for table_name in target_tables:
            profile = store.get_layer_profile(pid, table_name)
            if not profile:
                try:
                    gdb_path = store.get_gdb_path(pid)
                    records = _read_attach_table(gdb_path, table_name)
                except Exception:
                    records = []
            else:
                records = []

            table_types: dict[str, int] = {}
            table_size = 0
            for r in records:
                ct = r.get("content_type") or "unknown"
                table_types[ct] = table_types.get(ct, 0) + 1
                type_counts[ct] = type_counts.get(ct, 0) + 1
                table_size += r.get("data_size") or 0

            stats_by_table[table_name] = {
                "count": len(records),
                "types": table_types,
                "total_size_bytes": table_size,
            }
            total_count += len(records)
            total_size += table_size

        return json.dumps({
            "project": pid,
            "total_attachments": total_count,
            "total_size_bytes": total_size,
            "types_summary": type_counts,
            "by_table": stats_by_table,
            "geo_context_query_hint": (
                "Для поиска по содержимому вложений (PDF-карточки изученности, отчёты) "
                "используйте инструмент geo_context_query. "
                "Пример: geo_context_query(question='скважины глубиной более 1000м') — "
                "данные из PDF индексированы в граф знаний и доступны через семантический поиск."
            ),
        }, ensure_ascii=False, indent=2)

    return [list_attachments]


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def _read_attach_table(gdb_path: str, table_name: str) -> list[dict]:
    """Прочитать метаданные из таблицы *__ATTACH."""
    records = []
    try:
        with fiona.open(gdb_path, layer=table_name) as src:
            for i, feat in enumerate(src):
                props = dict(feat.get("properties") or {})
                records.append({
                    "index": i,
                    "att_name": props.get("ATT_NAME") or props.get("att_name") or "",
                    "content_type": props.get("CONTENT_TYPE") or props.get("content_type") or "",
                    "data_size": props.get("DATA_SIZE") or props.get("data_size") or 0,
                    "rel_globalid": props.get("REL_GLOBALID") or props.get("rel_globalid"),
                    "table": table_name,
                })
    except Exception:
        pass
    return records


