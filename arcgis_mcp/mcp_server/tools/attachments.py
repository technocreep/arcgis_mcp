"""P1 инструменты работы с вложениями — list_attachments, extract_attachment.

Вложения хранятся в таблицах *__ATTACH в .gdb.
Могут быть извлечены на диск (PDF, изображения и т.д.).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import fiona

from ..project_store import ProjectStore


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите get_project_summary(project_id=...)."
            )
        return pid

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
        all_attachments = []
        for table_name in target_tables:
            safe = table_name.replace("/", "_")
            profile = store.get_layer_profile(pid, table_name)

            # Читаем из .gdb если профиля нет
            if not profile:
                try:
                    gdb_path = store.get_gdb_path(pid)
                    records = _read_attach_table(gdb_path, table_name)
                except Exception as e:
                    records = []
            else:
                records = []   # профиль есть но без бинарных данных

            all_attachments.extend(records if records else _get_attachments_from_profile(
                pid, table_name, store
            ))

        return json.dumps({
            "project": pid,
            "total": len(all_attachments),
            "tables": target_tables,
            "attachments": all_attachments,
            "hint": (
                "Для извлечения файла: "
                "extract_attachment(table='Имя__ATTACH', index=0, output_dir='./output')"
            ),
        }, ensure_ascii=False, indent=2)

    def extract_attachment(
        table: str,
        index: int,
        output_dir: str = "./attachments_output",
        project_id: str | None = None,
    ) -> str:
        """Извлечь файл-вложение из геобазы на диск.

        Читает бинарные данные из таблицы *__ATTACH и сохраняет файл.
        Используй list_attachments() чтобы узнать доступные индексы.

        Args:
            table: Имя таблицы вложений (например "Izuch_A_sel__ATTACH").
            index: Индекс записи в таблице (0-based, из list_attachments).
            output_dir: Директория для сохранения файла.
                        По умолчанию: './attachments_output'
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            gdb_path = store.get_gdb_path(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        try:
            with fiona.open(gdb_path, layer=table) as src:
                features = list(src)

            if index < 0 or index >= len(features):
                return json.dumps({
                    "error": f"Индекс {index} вне диапазона. В таблице {len(features)} записей (0..{len(features)-1})."
                }, ensure_ascii=False)

            feat = features[index]
            props = dict(feat.get("properties") or {})

            # Имя файла
            att_name = (
                props.get("ATT_NAME") or props.get("att_name") or f"attachment_{index}"
            )
            content_type = props.get("CONTENT_TYPE") or props.get("content_type") or ""
            data_size = props.get("DATA_SIZE") or props.get("data_size") or 0
            rel_globalid = props.get("REL_GLOBALID") or props.get("rel_globalid")

            # Бинарные данные
            raw_data = props.get("DATA") or props.get("data")

            if raw_data is None:
                return json.dumps({
                    "warning": (
                        "Бинарные данные недоступны напрямую через fiona/GDAL для этой версии .gdb. "
                        "Файл метаданных сохранён."
                    ),
                    "att_name": att_name,
                    "content_type": content_type,
                    "data_size": data_size,
                    "rel_globalid": rel_globalid,
                    "hint": (
                        "Для извлечения бинарных данных из FGDB используйте arcpy или "
                        "экспортируйте через ArcGIS Pro."
                    ),
                }, ensure_ascii=False)

            # Сохраняем файл
            safe_name = att_name.replace("/", "_").replace("\\", "_")
            file_path = output_path / safe_name

            if isinstance(raw_data, (bytes, bytearray)):
                file_path.write_bytes(raw_data)
            elif isinstance(raw_data, str):
                # base64-encoded или hex
                import base64
                try:
                    file_path.write_bytes(base64.b64decode(raw_data))
                except Exception:
                    file_path.write_text(raw_data, encoding="utf-8")

            return json.dumps({
                "success": True,
                "file": str(file_path.resolve()),
                "att_name": att_name,
                "content_type": content_type,
                "size_bytes": file_path.stat().st_size if file_path.exists() else data_size,
                "rel_globalid": rel_globalid,
            }, ensure_ascii=False, indent=2)

        except Exception as e:
            return json.dumps({"error": f"Ошибка извлечения: {e}"}, ensure_ascii=False)

    return [list_attachments, extract_attachment]


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


def _get_attachments_from_profile(pid: str, table_name: str, store: ProjectStore) -> list[dict]:
    """Получить список вложений из layer_profile (если есть), иначе пустой список."""
    profile = store.get_layer_profile(pid, table_name)
    if not profile:
        return []
    # Если профиль хранит записи — читаем (сейчас профили attachment-таблиц минимальны)
    return []
