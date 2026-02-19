"""P0 инструменты инвентаризации — работают только по manifest.json.

list_projects, get_project_summary, list_layers, describe_layer
"""

from __future__ import annotations

import json
from typing import Callable

from ..project_store import ProjectStore


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    """Вернуть список P0-инструментов, связанных с хранилищем и состоянием."""

    def _resolve_project(project_id: str | None) -> str:
        """Вернуть project_id из параметра или текущего контекста."""
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите list_projects() и затем "
                "get_project_summary(project_id=...) чтобы выбрать проект."
            )
        return pid

    # -------------------------------------------------------------------
    # list_projects
    # -------------------------------------------------------------------
    def list_projects() -> str:
        """Показать список всех доступных GIS-проектов.

        Возвращает краткий список: id, название, количество слоёв.
        Используй этот инструмент первым при любом запросе пользователя о данных.
        """
        projects = store.list_projects()
        if not projects:
            return json.dumps({"projects": [], "message": "Нет загруженных проектов."}, ensure_ascii=False)

        return json.dumps({
            "projects": [
                {
                    "id": p.id,
                    "name": p.name,
                    "layers_count": p.layers_count,
                    "has_attachments": p.has_attachments,
                    "created_at": p.created_at,
                }
                for p in projects
            ],
            "hint": "Для работы с проектом вызови get_project_summary(project_id=...)"
        }, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------------
    # get_project_summary
    # -------------------------------------------------------------------
    def get_project_summary(project_id: str) -> str:
        """Получить сводку по проекту и установить его как текущий.

        Вызывай после list_projects() чтобы выбрать проект для работы.
        После вызова все другие инструменты автоматически работают с этим проектом.

        Args:
            project_id: Идентификатор проекта из list_projects()
        """
        try:
            manifest = store.get_manifest(project_id)
        except FileNotFoundError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        # Устанавливаем текущий проект
        state["current_project_id"] = project_id

        proj = manifest.get("project", {})
        quality = manifest.get("quality", {})
        mapping_q = manifest.get("mapping_quality", {})
        layers = manifest.get("layers", [])
        groups = manifest.get("groups", {})
        attachments = manifest.get("attachments_summary", {})

        # Группируем слои по группам для краткой сводки
        groups_summary = {}
        for layer in layers:
            group = layer.get("group", "— без группы —")
            groups_summary.setdefault(group, [])
            groups_summary[group].append(layer.get("display_name", layer["layer_id"]))

        result = {
            "project_id": project_id,
            "name": proj.get("name"),
            "map": proj.get("map", {}),
            "layers_total": quality.get("layers_total", len(layers)),
            "layers_non_empty": quality.get("layers_non_empty"),
            "mapping_coverage": f"{mapping_q.get('coverage_percent', 0)}%",
            "mapping_breakdown": {
                "from_aprx":     mapping_q.get("mapped_from_aprx", 0),
                "from_dict":     mapping_q.get("mapped_from_dict", 0),
                "from_inferred": mapping_q.get("mapped_from_inferred", 0),
                "needs_review":  mapping_q.get("needs_review", 0),
            },
            "groups": list(groups.keys()),
            "has_attachments": attachments.get("total", 0) > 0,
            "attachments_count": attachments.get("total", 0),
            "crs": quality.get("primary_crs"),
            "has_3d_layers": quality.get("has_3d_layers", False),
            "metadata_completeness": quality.get("metadata_completeness"),
            "layers_by_group": groups_summary,
            "status": f"✓ Проект '{project_id}' выбран как текущий",
        }

        if quality.get("warnings"):
            result["warnings"] = quality["warnings"][:5]   # не перегружать

        return json.dumps(result, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------------
    # list_layers
    # -------------------------------------------------------------------
    def list_layers(
        group: str | None = None,
        include_needs_review: bool = True,
        project_id: str | None = None,
    ) -> str:
        """Показать список слоёв проекта.

        Всегда возвращает display_name — человекочитаемое название слоя.
        Для каждого слоя показывает: название, тип геометрии, количество объектов, группу.

        Args:
            group: Если указано — показать только слои этой группы
                   (например "Гравика R-42", "Магнитка R-42").
                   Список групп: в get_project_summary().
            include_needs_review: Включить слои без расшифровки (по умолчанию True).
            project_id: ID проекта (необязательно, если уже выбран через get_project_summary).
        """
        try:
            pid = _resolve_project(project_id)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        layers = manifest.get("layers", [])

        # Фильтр по группе
        if group:
            group_lower = group.lower()
            layers = [
                l for l in layers
                if (l.get("group") or "").lower() == group_lower
            ]
            if not layers:
                # Нечёткий поиск группы
                all_groups = {l.get("group") for l in manifest.get("layers", []) if l.get("group")}
                close = [g for g in all_groups if group_lower in g.lower()]
                hint = f"Группа '{group}' не найдена."
                if close:
                    hint += f" Возможно имелось в виду: {close}"
                return json.dumps({"error": hint, "available_groups": list(all_groups)}, ensure_ascii=False)

        # Фильтр needs_review
        if not include_needs_review:
            layers = [l for l in layers if not l.get("needs_review")]

        result_layers = []
        for l in layers:
            entry = {
                "display_name": l.get("display_name", l["layer_id"]),
                "layer_id": l["layer_id"],
                "geometry_type": l.get("geometry_type"),
                "feature_count": l.get("feature_count", 0),
            }
            if l.get("group"):
                entry["group"] = l["group"]
            if l.get("units"):
                entry["units"] = l["units"]
            if l.get("needs_review"):
                entry["needs_review"] = True
                entry["note"] = "Нет расшифровки из проекта, название может быть неточным"
            result_layers.append(entry)

        return json.dumps({
            "project": pid,
            "layers_count": len(result_layers),
            "layers": result_layers,
            "hint": "Для деталей по слою: describe_layer(layer='display_name или layer_id')"
        }, ensure_ascii=False, indent=2)

    # -------------------------------------------------------------------
    # describe_layer
    # -------------------------------------------------------------------
    def describe_layer(
        layer: str,
        project_id: str | None = None,
    ) -> str:
        """Подробное описание слоя: поля, статистика, CRS, extent, вложения.

        Принимает display_name, layer_id или alias — автоматически определяет слой.
        Для числовых полей показывает диапазон с единицами измерения.

        Args:
            layer: Название слоя (display_name, layer_id или alias).
                   Примеры: "гравика", "gms_r", "Поле дельта G (мГал)", "скважины".
            project_id: ID проекта (необязательно, если уже выбран).
        """
        try:
            pid = _resolve_project(project_id)
            manifest = store.get_manifest(pid)
        except (ValueError, FileNotFoundError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        # Резолвим имя слоя
        layer_id = store.resolve_layer_name(pid, layer)
        if layer_id is None:
            # Подсказка — показываем похожие слои
            all_names = [
                l.get("display_name", l["layer_id"])
                for l in manifest.get("layers", [])
            ]
            return json.dumps({
                "error": f"Слой '{layer}' не найден.",
                "hint": "Попробуйте list_layers() чтобы увидеть все доступные слои.",
                "similar": [n for n in all_names if layer.lower()[:4] in n.lower()][:5],
            }, ensure_ascii=False)

        layer_entry = store.get_layer_entry(manifest, layer_id)
        if layer_entry is None:
            return json.dumps({"error": f"Слой '{layer_id}' не найден в manifest."}, ensure_ascii=False)

        # Детальный профиль из layer_profiles/ (если есть)
        profile = store.get_layer_profile(pid, layer_id)

        units = layer_entry.get("units")

        # Форматируем поля с учётом единиц
        fields_formatted = []
        fields_source = (profile or layer_entry).get("fields", [])
        for f in fields_source:
            field_info: dict = {
                "name": f["name"],
                "type": f.get("dtype", ""),
            }
            if f.get("alias"):
                field_info["alias"] = f["alias"]
            if f.get("nulls"):
                field_info["nulls"] = f["nulls"]
            # Числовая статистика с единицами
            if f.get("min") is not None:
                suffix = f" {units}" if units else ""
                field_info["range"] = f"{f['min']:.4g} … {f['max']:.4g}{suffix}"
                if f.get("mean") is not None:
                    field_info["mean"] = f"{f['mean']:.4g}{suffix}"
            # Категориальная статистика
            if f.get("unique_count") is not None:
                field_info["unique_values"] = f["unique_count"]
            if f.get("top_values"):
                field_info["top_values"] = f["top_values"]
            fields_formatted.append(field_info)

        result: dict = {
            "layer_id": layer_id,
            "display_name": layer_entry.get("display_name", layer_id),
            "display_name_source": layer_entry.get("display_name_source"),
            "group": layer_entry.get("group"),
            "feature_dataset": layer_entry.get("feature_dataset"),
            "geometry_type": layer_entry.get("geometry_type"),
            "feature_count": layer_entry.get("feature_count", 0),
            "crs_epsg": layer_entry.get("crs_epsg"),
            "extent_wgs84": layer_entry.get("extent_wgs84"),
            "units": units,
            "fields": fields_formatted,
        }

        if layer_entry.get("needs_review"):
            result["warning"] = (
                "Для этого слоя нет расшифровки из проекта (.aprx). "
                "Техническое имя может не отражать содержимое данных."
            )

        if layer_entry.get("attachments"):
            result["attachments"] = layer_entry["attachments"]

        if layer_entry.get("label_expression"):
            result["label_expression"] = layer_entry["label_expression"]

        return json.dumps(result, ensure_ascii=False, indent=2)

    return [list_projects, get_project_summary, list_layers, describe_layer]
