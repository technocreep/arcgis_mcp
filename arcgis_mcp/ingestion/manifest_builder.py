"""Сборка manifest.json по спецификации MANIFEST_SPEC.md.

Объединяет результаты всех парсеров в единый JSON-документ.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from config import MANIFEST_VERSION, PIPELINE_VERSION
from .mapping import LayerMappingResult, MappedLayer
from .parser_aprx import AprxData
from .parser_gdb import FieldProfile, GdbData, LayerProfile
from .quality import QualityReport


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _field_profile_to_dict(fp: FieldProfile) -> dict:
    d: dict[str, Any] = {"name": fp.name, "dtype": fp.dtype}
    if fp.nulls:
        d["nulls"] = fp.nulls
    if fp.min is not None:
        d["min"] = fp.min
    if fp.max is not None:
        d["max"] = fp.max
    if fp.mean is not None:
        d["mean"] = round(fp.mean, 6)
    if fp.std is not None:
        d["std"] = round(fp.std, 6)
    if fp.unique_count is not None:
        d["unique_count"] = fp.unique_count
    if fp.top_values:
        d["top_values"] = fp.top_values
    return d


def _layer_profile_by_id(gdb_data: GdbData) -> dict[str, LayerProfile]:
    return {lp.layer_id: lp for lp in gdb_data.layers}


# ---------------------------------------------------------------------------
# Генерация aliases
# ---------------------------------------------------------------------------

_TRANSLITERATION = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}

_SEMANTIC_ALIASES: dict[str, list[str]] = {
    # единицы → семантика
    "мгал": ["гравика", "гравитационное поле", "gravity"],
    "нтл": ["магнитка", "магнитное поле", "magnetics"],
    # ключевые слова
    "скважин": ["скважины", "бурение", "wells", "drillholes"],
    "канав": ["канавы", "траншеи", "trenches"],
    "изученность": ["работы", "исследования", "surveys"],
    "геологи": ["геология", "geology"],
    "разломы": ["тектоника", "tectonics", "faults"],
    "рек": ["реки", "гидрография", "rivers"],
    "озер": ["озёра", "lakes"],
    "дорог": ["дороги", "roads"],
    "линеамент": ["линеаменты", "lineaments"],
    "изолин": ["изолинии", "isolines", "contours"],
    "экстремум": ["экстремумы", "extrema"],
    "градиент": ["градиент", "gradient"],
}


def _transliterate(text: str) -> str:
    return "".join(_TRANSLITERATION.get(c, c) for c in text.lower())


def _generate_aliases(
    dataset_name: str,
    display_name: str,
    units: str | None,
) -> list[str]:
    aliases: set[str] = set()

    # Токены из display_name (lowercase)
    clean_display = re.sub(r"[^\w\s]", " ", display_name.lower())
    tokens = [t.strip() for t in clean_display.split() if len(t.strip()) > 2]
    aliases.update(tokens)

    # dataset_name без подчёркиваний
    aliases.add(dataset_name.lower().replace("_", " "))
    aliases.add(dataset_name.lower())

    # Транслит display_name
    aliases.add(_transliterate(display_name.split("(")[0].strip()))

    # Семантические синонимы по ключевым словам
    combined = (display_name + " " + dataset_name).lower()
    for keyword, synonyms in _SEMANTIC_ALIASES.items():
        if keyword in combined:
            aliases.update(synonyms)

    # Единицы → тип данных
    if units:
        units_lower = units.lower()
        if "мгал" in units_lower:
            aliases.update(["гравика", "гравиметрия", "gravity"])
        if "нтл" in units_lower:
            aliases.update(["магнитка", "магниторазведка", "magnetics"])

    # Убираем пустые и слишком короткие
    result = [a.strip() for a in aliases if len(a.strip()) >= 2]
    result = list(dict.fromkeys(result))   # дедупликация с сохранением порядка
    result.sort()
    return result


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def build_manifest(
    project_id: str,
    gdb_data: GdbData,
    aprx_data: AprxData | None,
    mapping: LayerMappingResult,
    quality: QualityReport,
    atbx_data: dict | None = None,
    source_files: dict | None = None,
) -> dict:
    """Собрать manifest.json по спецификации MANIFEST_SPEC.md.

    Args:
        project_id:    уникальный идентификатор проекта (slug)
        gdb_data:      результат parse_gdb()
        aprx_data:     результат parse_aprx() или None
        mapping:       результат build_mapping()
        quality:       результат compute_quality()
        atbx_data:     результат parse_atbx() или None
        source_files:  {gdb, aprx, atbx} — имена исходных файлов

    Returns:
        dict — готовый manifest (для сохранения через json.dumps)
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if source_files is None:
        source_files = {}

    lp_index = _layer_profile_by_id(gdb_data)

    # -----------------------------------------------------------------------
    # project.map
    # -----------------------------------------------------------------------
    map_info: dict = {}
    if aprx_data:
        map_info["name"] = aprx_data.map_name

        # primary_crs из quality
        if quality.primary_crs:
            # Добавим человекочитаемое имя CRS если знаем
            crs_label = _crs_label(quality.primary_crs)
            map_info["primary_crs"] = crs_label

        if aprx_data.datum_transforms:
            map_info["datum_transforms"] = aprx_data.datum_transforms

    # extent_wgs84 — берём максимальный bbox по всем слоям с геометрией
    global_extent = _compute_global_extent(gdb_data)
    if global_extent:
        map_info["extent_wgs84"] = global_extent

    # -----------------------------------------------------------------------
    # groups — из aprx, обогащённые маппингом
    # -----------------------------------------------------------------------
    groups_dict: dict = {}
    if aprx_data and aprx_data.groups:
        for group_name, ds_names in aprx_data.groups.items():
            group_entry: dict = {"layers": ds_names}
            groups_dict[group_name] = group_entry

    # -----------------------------------------------------------------------
    # layers — объединяем mapping + gdb profiles
    # -----------------------------------------------------------------------
    layers_list: list[dict] = []
    aliases_dict: dict[str, list[str]] = {}

    for ml in mapping.mapped:
        ds_name = ml.dataset_name
        lp = lp_index.get(ds_name)

        layer_entry: dict = {
            "layer_id": ds_name,
            "display_name": ml.display_name,
            "display_name_source": ml.display_name_source,
        }

        # Флаг для агента: предупреждать пользователя при обращении к этому слою
        if ml.needs_review:
            layer_entry["needs_review"] = True

        if ml.group:
            layer_entry["group"] = ml.group
        if ml.feature_dataset:
            layer_entry["feature_dataset"] = ml.feature_dataset
        if ml.units:
            layer_entry["units"] = ml.units

        if lp:
            if lp.geometry_type:
                layer_entry["geometry_type"] = lp.geometry_type
            layer_entry["feature_count"] = lp.feature_count
            if lp.crs_epsg:
                layer_entry["crs_epsg"] = lp.crs_epsg
            if lp.extent_wgs84:
                layer_entry["extent_wgs84"] = lp.extent_wgs84
            layer_entry["is_large"] = lp.is_large

            # fields
            fields_list = [_field_profile_to_dict(fp) for fp in lp.fields]
            # применяем алиасы полей из .aprx
            if ml.field_aliases:
                for fd in fields_list:
                    alias = ml.field_aliases.get(fd["name"])
                    if alias:
                        fd["alias"] = alias
            layer_entry["fields"] = fields_list

            # attachments info
            for at in gdb_data.attachment_tables:
                if at.parent_layer == ds_name:
                    layer_entry["attachments"] = {
                        "table": at.table_name,
                        "count": at.total_attachments,
                        "link_field": "REL_GLOBALID",
                        "content_types": _count_content_types(at.attachments),
                    }
                    break

        # aprx-специфика
        if ml.aprx_file:
            source_block: dict = {"from_gdb": True, "from_aprx": True, "aprx_file": ml.aprx_file}
            layer_entry["source"] = source_block
            if ml.aprx_visibility is not None:
                layer_entry["visibility_in_project"] = ml.aprx_visibility
            if ml.aprx_display_field:
                layer_entry["display_field"] = ml.aprx_display_field
            if ml.aprx_label_expression:
                layer_entry["label_expression"] = ml.aprx_label_expression
        else:
            layer_entry["source"] = {"from_gdb": True, "from_aprx": False}

        layers_list.append(layer_entry)

        # Генерируем aliases
        aliases = _generate_aliases(ds_name, ml.display_name, ml.units)
        if aliases:
            aliases_dict[ds_name] = aliases

    # -----------------------------------------------------------------------
    # attachments_summary
    # -----------------------------------------------------------------------
    all_attachments_count = sum(at.total_attachments for at in gdb_data.attachment_tables)
    all_content_types: dict[str, int] = {}
    for at in gdb_data.attachment_tables:
        for ct, cnt in _count_content_types(at.attachments).items():
            all_content_types[ct] = all_content_types.get(ct, 0) + cnt

    attachments_summary: dict = {
        "total": all_attachments_count,
        "tables": [at.table_name for at in gdb_data.attachment_tables],
        "content_types": all_content_types,
        "extractable": all_attachments_count > 0,
    }

    # -----------------------------------------------------------------------
    # layer_mapping (для отладки)
    # -----------------------------------------------------------------------
    layer_mapping_list = []
    for ml in mapping.mapped:
        layer_mapping_list.append({
            "dataset_name": ml.dataset_name,
            "display_name": ml.display_name,
            "display_name_source": ml.display_name_source,
            "group": ml.group,
            "feature_dataset": ml.feature_dataset,
            "description": ml.description,
            "units": ml.units,
            "aprx_file": ml.aprx_file,
            "aprx_visibility": ml.aprx_visibility,
            "aprx_display_field": ml.aprx_display_field,
            "aprx_label_expression": ml.aprx_label_expression,
        })

    unmapped_list = [
        {
            "dataset_name": u.dataset_name,
            "reason": u.reason,
            "display_name": u.display_name,
            "display_name_source": u.display_name_source,
            "needs_review": u.needs_review,
        }
        for u in mapping.unmapped
    ]

    mapping_quality_dict = {
        "total_gdb_layers": mapping.quality.total_gdb_layers,
        "mapped_from_aprx": mapping.quality.mapped_from_aprx,
        "mapped_from_dict": mapping.quality.mapped_from_dict,
        "mapped_from_inferred": mapping.quality.mapped_from_inferred,
        "needs_review": mapping.quality.unmapped,
        "coverage_percent": mapping.quality.coverage_percent,
        "has_groups": mapping.quality.has_groups,
        "groups_count": mapping.quality.groups_count,
    }

    # -----------------------------------------------------------------------
    # quality
    # -----------------------------------------------------------------------
    quality_dict = {
        "layers_total": quality.layers_total,
        "layers_non_empty": quality.layers_non_empty,
        "layers_with_display_name": quality.layers_with_display_name,
        "layers_with_unknown_meaning": quality.layers_with_unknown_meaning,
        "attachments_extractable": quality.attachments_extractable,
        "crs_consistent": quality.crs_consistent,
        "primary_crs": quality.primary_crs,
        "has_3d_layers": quality.has_3d_layers,
        "has_rasters": quality.has_rasters,
        "metadata_completeness": quality.metadata_completeness,
        "warnings": quality.warnings,
    }

    # -----------------------------------------------------------------------
    # Финальная сборка
    # -----------------------------------------------------------------------
    manifest: dict = {
        "version": MANIFEST_VERSION,
        "generated_at": now,
        "generator": f"gis-ingestion-pipeline v{PIPELINE_VERSION}",

        "project": {
            "id": project_id,
            "name": project_id,   # будет обновлён из aprx map_name или пользователем
            "source_files": {
                "gdb": source_files.get("gdb"),
                "aprx": source_files.get("aprx"),
                "atbx": source_files.get("atbx"),
            },
            "map": map_info,
        },

        "groups": groups_dict,

        "layers": layers_list,

        "layer_mapping": layer_mapping_list,
        "unmapped_layers": unmapped_list,
        "mapping_quality": mapping_quality_dict,

        "attachments_summary": attachments_summary,

        "toolbox": atbx_data,

        "quality": quality_dict,

        "aliases": aliases_dict,
    }

    # Если есть имя карты из .aprx — используем как название проекта
    if aprx_data and aprx_data.map_name:
        manifest["project"]["name"] = aprx_data.map_name

    return manifest


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def _count_content_types(attachments) -> dict[str, int]:
    counts: dict[str, int] = {}
    for att in attachments:
        ct = att.content_type or "unknown"
        counts[ct] = counts.get(ct, 0) + 1
    return counts


def _compute_global_extent(gdb_data: GdbData) -> dict | None:
    """Объединить bbox всех слоёв в один глобальный."""
    lons_min, lats_min, lons_max, lats_max = [], [], [], []
    for lp in gdb_data.layers:
        if lp.extent_wgs84:
            e = lp.extent_wgs84
            lons_min.append(e["min_lon"])
            lats_min.append(e["min_lat"])
            lons_max.append(e["max_lon"])
            lats_max.append(e["max_lat"])
    if not lons_min:
        return None
    return {
        "min_lon": min(lons_min),
        "min_lat": min(lats_min),
        "max_lon": max(lons_max),
        "max_lat": max(lats_max),
    }


_CRS_LABELS = {
    "EPSG:7683": "EPSG:7683 (ГСК-2011)",
    "EPSG:4326": "EPSG:4326 (WGS 84)",
    "EPSG:32637": "EPSG:32637 (WGS 84 / UTM zone 37N)",
    "EPSG:32638": "EPSG:32638 (WGS 84 / UTM zone 38N)",
    "EPSG:4284": "EPSG:4284 (Pulkovo 1942)",
}


def _crs_label(epsg_str: str) -> str:
    return _CRS_LABELS.get(epsg_str, epsg_str)
