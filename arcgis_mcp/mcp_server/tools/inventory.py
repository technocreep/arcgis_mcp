"""P0 инструменты инвентаризации — работают только по manifest.json.

list_projects, get_project_summary, list_layers, describe_layer
"""

from __future__ import annotations

import json
from typing import Callable

from ..project_store import ProjectStore


def _fmt_field(f: dict, units: str | None) -> str:
    """Форматировать поле слоя в одну компактную строку."""
    name = f["name"]
    dtype = f.get("dtype", "")
    nulls = f.get("nulls") or 0

    null_str = f" [{nulls} nulls]" if nulls else ""

    # Числовое поле
    if f.get("min") is not None:
        suffix = f" {units}" if units else ""
        range_str = f"{f['min']:.4g}…{f['max']:.4g}{suffix}"
        mean_str = f"  mean:{f['mean']:.4g}{suffix}" if f.get("mean") is not None else ""
        return f"{name}  {dtype}  {range_str}{mean_str}{null_str}"

    # Категориальное поле
    if f.get("unique_count") is not None:
        top = f.get("top_values") or {}
        top_str = "/".join(list(top.keys())[:4])
        if len(top) > 4:
            top_str += "/…"
        return f"{name}  {dtype}  {f['unique_count']} uniq{null_str}: {top_str}"

    return f"{name}  {dtype}{null_str}"


def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    """Вернуть список P0-инструментов, связанных с хранилищем и состоянием."""

    def _resolve_project(project_id: str | None) -> str:
        if not project_id:
            raise ValueError("project_id обязателен.")
        return project_id

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
        }, ensure_ascii=False, indent=2)

    def get_project_summary(project_id: str) -> str:
        """Получить сводку по проекту: слои, группы, CRS, вложения.

        Вызывай после list_projects() для получения метаданных проекта.
        Используй project_id из этого ответа во всех последующих инструментах.

        Args:
            project_id: Идентификатор проекта из list_projects()
        """
        try:
            manifest = store.get_manifest(project_id)
        except FileNotFoundError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        proj = manifest.get("project", {})
        quality = manifest.get("quality", {})
        mapping_q = manifest.get("mapping_quality", {})
        layers = manifest.get("layers", [])
        attachments = manifest.get("attachments_summary", {})

        # Группируем слои по группам для краткой сводки
        groups_summary: dict[str, int] = {}
        for layer in layers:
            group = layer.get("group", "— без группы —")
            groups_summary[group] = groups_summary.get(group, 0) + 1

        # Компактный объект карты: только имя и bbox
        map_raw = proj.get("map", {})
        ext = map_raw.get("extent_wgs84", {})
        map_info: dict = {"name": map_raw.get("name")}
        if ext:
            map_info["extent_wgs84"] = [
                ext.get("min_lon"), ext.get("min_lat"),
                ext.get("max_lon"), ext.get("max_lat"),
            ]

        result = {
            "project_id": project_id,
            "name": proj.get("name"),
            "map": map_info,
            "layers_total": quality.get("layers_total", len(layers)),
            "layers_non_empty": quality.get("layers_non_empty"),
            "mapping_coverage": f"{mapping_q.get('coverage_percent', 0)}%",
            "groups": groups_summary,
            "attachments_count": attachments.get("total", 0),
            "crs": quality.get("primary_crs"),
            "has_3d_layers": quality.get("has_3d_layers", False),
            "metadata_completeness": quality.get("metadata_completeness"),
        }

        if quality.get("warnings"):
            result["warnings"] = quality["warnings"][:5]   # не перегружать

        return json.dumps(result, ensure_ascii=False)

    def list_layers(
        group: str | None = None,
        include_needs_review: bool = True,
        project_id: str | None = None,
        output_format: str = "compact",
    ) -> str:
        """Показать список слоёв проекта.

        Всегда возвращает display_name — человекочитаемое название слоя.
        По умолчанию возвращает компактный текст (экономит контекст).

        Args:
            group: Если указано — показать только слои этой группы
                   (например "Гравика R-42", "Магнитка R-42").
                   Список групп: в get_project_summary().
            include_needs_review: Включить слои без расшифровки (по умолчанию True).
            project_id: ID проекта из list_projects().
            output_format: "compact" (по умолчанию) — текст, сгруппированный по группам;
                           "json" — полный JSON для отладки.
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
            result_layers.append(entry)

        # Компактный текстовый формат (экономит контекст)
        if output_format != "json":
            from collections import defaultdict
            by_group: dict[str, list[str]] = defaultdict(list)
            ungrouped: list[str] = []
            for entry in result_layers:
                geom = entry.get("geometry_type") or "?"
                line = f"  {entry['layer_id']}  {entry['display_name']}  [{geom}]"
                if entry.get("needs_review"):
                    line += "  ⚠"
                grp = entry.get("group")
                if grp:
                    by_group[grp].append(line)
                else:
                    ungrouped.append(line)
            lines = [
                f"project={pid}  layers={len(result_layers)}",
            ]
            for grp, entries in by_group.items():
                lines.append(f"\n[{grp}]")
                lines.extend(entries)
            if ungrouped:
                lines.append("\n[без группы]")
                lines.extend(ungrouped)
            return "\n".join(lines)

        return json.dumps({
            "project": pid,
            "layers_count": len(result_layers),
            "layers": result_layers,
            "hint": "Для деталей по слою: describe_layer(layer='display_name или layer_id')"
        }, ensure_ascii=False)

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
            project_id: ID проекта из list_projects().
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

        # Компактный формат полей: одна строка на поле
        fields_source = (profile or layer_entry).get("fields", [])
        fields_formatted = [_fmt_field(f, units) for f in fields_source]

        # extent_wgs84 как массив вместо словаря
        ext = layer_entry.get("extent_wgs84") or {}
        extent_arr = (
            [ext.get("min_lon"), ext.get("min_lat"), ext.get("max_lon"), ext.get("max_lat")]
            if ext else None
        )

        result: dict = {
            "layer_id": layer_id,
            "display_name": layer_entry.get("display_name", layer_id),
            "group": layer_entry.get("group"),
            "geometry_type": layer_entry.get("geometry_type"),
            "feature_count": layer_entry.get("feature_count", 0),
            "crs_epsg": layer_entry.get("crs_epsg"),
            "extent_wgs84": extent_arr,
            "units": units,
            "fields": fields_formatted,
        }

        # display_name_source только если не стандартный источник
        src = layer_entry.get("display_name_source")
        if src and src != "aprx":
            result["display_name_source"] = src

        # feature_dataset только если задан
        if layer_entry.get("feature_dataset"):
            result["feature_dataset"] = layer_entry["feature_dataset"]

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
