"""Построение Knowledge Graph из manifest и PDF-вложений.

Функции:
    build_from_manifest      — заполнить KG из manifest.json проекта
    index_pdf_attachments    — распарсить PDF карточки изученности из .gdb
    update_datacube_blocks   — добавить DatacubeBlock узлы после Data Cube job
    delete_project_subgraph  — удалить все узлы проекта из KG
"""

from __future__ import annotations

import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import fiona

from .kg_client import Neo4jClient
from .pdf_parser import parse_investigation_card
from .tile_builder import build_tiles

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Основной builder: manifest → KG
# ---------------------------------------------------------------------------

def build_from_manifest(manifest: dict, project_id: str, kg: Neo4jClient):
    """Построить / обновить KG из manifest.json проекта.

    Создаёт узлы: Project, Group, Layer, Field, Attachment.
    Для слоёв с is_large=True строит SpatialTile (опционально, требует gdb_path).
    """
    logger.info("[KG] Индексирование проекта %s", project_id)

    proj = manifest.get("project", {})
    map_info = proj.get("map", {})

    # --- Project ---
    extent = map_info.get("extent_wgs84") or {}
    kg.merge_node("Project", {"id": project_id}, {
        "name": proj.get("name", project_id),
        "primary_crs": str(map_info.get("primary_crs", "")),
        "extent_json": json.dumps(extent),
    })

    # --- Groups ---
    # manifest["groups"] is a dict: {group_name: {"layers": [...]}}
    groups = manifest.get("groups", {})
    for group_name, grp_data in groups.items():
        if not group_name:
            continue
        group_id = f"{project_id}::{group_name}"
        kg.merge_node("Group", {"id": group_id}, {
            "name": group_name,
            "project_id": project_id,
            "feature_dataset": "",
        })
        kg.merge_rel("Project", "id", project_id, "HAS_GROUP", "Group", "id", group_id)

    # --- Layers + Fields + Attachments ---
    layers = manifest.get("layers", [])
    # manifest["layer_mapping"] is a list; build a lookup dict by dataset_name
    layer_mapping_raw = manifest.get("layer_mapping", [])
    layer_mapping = {e["dataset_name"]: e for e in layer_mapping_raw if "dataset_name" in e}
    attachments_summary = manifest.get("attachments_summary", {})
    attach_tables = set(attachments_summary.get("tables", []))

    for layer_entry in layers:
        layer_id = layer_entry.get("layer_id", "")
        if not layer_id:
            continue

        lm = layer_mapping.get(layer_id, {})
        extent_wgs84 = layer_entry.get("extent_wgs84") or {}

        kg.merge_node("Layer", {"id": layer_id}, {
            "project_id": project_id,
            "display_name": layer_entry.get("display_name") or lm.get("display_name") or layer_id,
            "geometry_type": layer_entry.get("geometry_type") or "",
            "feature_count": layer_entry.get("feature_count") or 0,
            "extent_json": json.dumps(extent_wgs84),
            "crs_epsg": layer_entry.get("crs_epsg") or 0,
            "is_large": bool(layer_entry.get("is_large", False)),
            "group_name": layer_entry.get("group") or "",
            "feature_dataset": layer_entry.get("feature_dataset") or "",
            "units": lm.get("units") or "",
        })

        kg.merge_rel("Project", "id", project_id, "HAS_LAYER", "Layer", "id", layer_id)

        # Связь с группой
        grp_name = layer_entry.get("group")
        if grp_name:
            group_id = f"{project_id}::{grp_name}"
            kg.merge_rel("Layer", "id", layer_id, "IN_GROUP", "Group", "id", group_id)

        # --- Fields ---
        for fld in layer_entry.get("fields", []):
            field_name = fld.get("name", "")
            if not field_name:
                continue
            field_id = f"{layer_id}::{field_name}"
            kg.merge_node("Field", {"id": field_id}, {
                "layer_id": layer_id,
                "project_id": project_id,
                "name": field_name,
                "dtype": fld.get("dtype") or "",
                "nulls": fld.get("nulls") or 0,
                "min_val": _safe_float(fld.get("min")),
                "max_val": _safe_float(fld.get("max")),
                "mean": _safe_float(fld.get("mean")),
                "std": _safe_float(fld.get("std")),
                "unique_count": fld.get("unique_count") or 0,
                "top_values_json": json.dumps(fld.get("top_values") or {}, ensure_ascii=False),
            })
            kg.merge_rel("Layer", "id", layer_id, "HAS_FIELD", "Field", "id", field_id)

        # --- Attachment metadata (без бинарных данных) ---
        attach_table = f"{layer_id}__ATTACH"
        if attach_table in attach_tables:
            att_id = f"{project_id}::{attach_table}"
            kg.merge_node("Attachment", {"id": att_id}, {
                "layer_id": layer_id,
                "project_id": project_id,
                "att_name": attach_table,
                "content_type": "table",
                "data_size": 0,
                "rel_globalid": "",
            })
            kg.merge_rel("Layer", "id", layer_id, "HAS_ATTACHMENT", "Attachment", "id", att_id)

    logger.info("[KG] Проект %s: %d слоёв проиндексировано", project_id, len(layers))


# ---------------------------------------------------------------------------
# PDF карточки изученности
# ---------------------------------------------------------------------------

_PDF_PARSE_WORKERS = 4  # макс. параллельных LLM-запросов


def index_pdf_attachments(project_id: str, gdb_path: str, manifest: dict, kg: Neo4jClient):
    """Извлечь PDF-вложения из .gdb, распарсить карточки, загрузить в KG.

    Парсинг карточек выполняется параллельно (до _PDF_PARSE_WORKERS потоков),
    запись в KG — последовательно.
    """
    attach_summary = manifest.get("attachments_summary", {})
    attach_tables = attach_summary.get("tables", [])

    if not attach_tables:
        logger.info("[KG] Нет таблиц вложений в проекте %s", project_id)
        return

    layers_by_id = {l["layer_id"]: l for l in manifest.get("layers", [])}

    # ── Фаза 1: создать Attachment-узлы и собрать PDF для парсинга ──────────
    # pdf_jobs: список (att_id, pdf_bytes, parent_layer_id, parent_extent)
    pdf_jobs: list[tuple[str, bytes, str, Any]] = []

    for table_name in attach_tables:
        parent_layer_id = table_name.replace("__ATTACH", "")
        parent_layer = layers_by_id.get(parent_layer_id, {})
        parent_extent = parent_layer.get("extent_wgs84")

        try:
            with fiona.open(gdb_path, layer=table_name) as src:
                features = list(src)
        except Exception as e:
            logger.warning("[KG] Ошибка чтения %s: %s", table_name, e)
            continue

        for i, feat in enumerate(features):
            props = dict(feat.get("properties") or {})
            content_type = props.get("CONTENT_TYPE") or props.get("content_type") or ""
            att_name = props.get("ATT_NAME") or props.get("att_name") or f"att_{i}"
            rel_globalid = props.get("REL_GLOBALID") or props.get("rel_globalid") or ""
            data_size = props.get("DATA_SIZE") or props.get("data_size") or 0

            att_id = f"{project_id}::{table_name}::{i}"
            kg.merge_node("Attachment", {"id": att_id}, {
                "layer_id": parent_layer_id,
                "project_id": project_id,
                "att_name": att_name,
                "content_type": content_type,
                "data_size": int(data_size),
                "rel_globalid": rel_globalid,
            })
            kg.merge_rel(
                "Layer", "id", parent_layer_id,
                "HAS_ATTACHMENT",
                "Attachment", "id", att_id,
            )

            if "pdf" not in content_type.lower() and not att_name.lower().endswith(".pdf"):
                continue

            pdf_bytes = _extract_binary(props)
            if not pdf_bytes:
                logger.debug("[KG] Нет бинарных данных для %s[%d]", table_name, i)
                continue

            pdf_jobs.append((att_id, pdf_bytes, parent_layer_id, parent_extent))

    if not pdf_jobs:
        logger.info("[KG] Проект %s: PDF вложений не найдено", project_id)
        return

    logger.info("[KG] Парсинг %d PDF карточек (до %d параллельно)...", len(pdf_jobs), _PDF_PARSE_WORKERS)

    # ── Фаза 2: параллельный парсинг карточек ────────────────────────────────
    # results: att_id -> (card, parent_layer_id, parent_extent)
    results: dict[str, tuple] = {}
    with ThreadPoolExecutor(max_workers=_PDF_PARSE_WORKERS) as pool:
        future_to_job = {
            pool.submit(parse_investigation_card, pdf_bytes): (att_id, parent_layer_id, parent_extent)
            for att_id, pdf_bytes, parent_layer_id, parent_extent in pdf_jobs
        }
        for future in as_completed(future_to_job):
            att_id, parent_layer_id, parent_extent = future_to_job[future]
            try:
                card = future.result()
            except Exception as e:
                logger.warning("[KG] Ошибка парсинга %s: %s", att_id, e)
                card = None
            if card and card.reg_number:
                results[att_id] = (card, parent_layer_id, parent_extent)
            else:
                logger.debug("[KG] Не распознана карточка %s", att_id)

    # ── Фаза 3: запись результатов в KG (последовательно) ───────────────────
    pdf_count = 0
    for att_id, (card, parent_layer_id, parent_extent) in results.items():
        kg.merge_node("InvestigationCard", {"reg_number": card.reg_number}, {
            "inventory_rosgeolfond": card.inventory_rosgeolfond,
            "inventory_tgf": card.inventory_tgf,
            "title": card.title,
            "authors": card.authors,
            "organization": card.organization,
            "year_start": card.year_start or 0,
            "year_end": card.year_end or 0,
            "purpose": card.purpose,
            "minerals_json": json.dumps(card.minerals, ensure_ascii=False),
            "reserves_calculated": card.reserves_calculated,
            "resources_calculated": card.resources_calculated,
            "work_type": card.work_type,
            "scale": card.scale,
            "abstract_methods": card.abstract_methods,
            "abstract_results": card.abstract_results,
            "abstract_conclusions": card.abstract_conclusions,
            "keywords_json": json.dumps(card.keywords, ensure_ascii=False),
            "area_km2": card.area_km2 or 0.0,
            "bbox_json": json.dumps(card.bbox or {}, ensure_ascii=False),
            "sheet_nomenclature": card.sheet_nomenclature,
            "region_okrug": card.region_okrug,
            "region_oblast": card.region_oblast,
            "completion_status": card.completion_status,
        })
        kg.merge_rel(
            "Attachment", "id", att_id,
            "IS_CARD",
            "InvestigationCard", "reg_number", card.reg_number,
        )
        for mineral in card.minerals:
            if not mineral:
                continue
            kg.merge_node("Mineral", {"name": mineral})
            kg.merge_rel(
                "InvestigationCard", "reg_number", card.reg_number,
                "TARGETS",
                "Mineral", "name", mineral,
            )
        if card.organization:
            org = card.organization[:100]
            kg.merge_node("Organization", {"name": org})
            kg.merge_rel(
                "InvestigationCard", "reg_number", card.reg_number,
                "CONDUCTED_BY",
                "Organization", "name", org,
            )
        if card.work_type:
            method_id = f"{card.work_type}::{card.scale}"
            kg.merge_node("WorkMethod", {"name": method_id}, {
                "work_type": card.work_type,
                "scale": card.scale,
            })
            kg.merge_rel(
                "InvestigationCard", "reg_number", card.reg_number,
                "USES_METHOD",
                "WorkMethod", "name", method_id,
            )
        if card.bbox and parent_extent:
            _link_card_to_layers(card.reg_number, card.bbox, manifest, project_id, kg)
        pdf_count += 1

    logger.info("[KG] Проект %s: %d PDF карточек проиндексировано", project_id, pdf_count)


def _link_card_to_layers(
    reg_number: str, card_bbox: dict, manifest: dict, project_id: str, kg: Neo4jClient
):
    """Создать SPATIALLY_COVERS рёбра для слоёв с bbox-пересечением."""
    cn = card_bbox.get("n", 0)
    cs = card_bbox.get("s", 0)
    ce = card_bbox.get("e", 0)
    cw = card_bbox.get("w", 0)

    for layer in manifest.get("layers", []):
        ext = layer.get("extent_wgs84") or {}
        ln = ext.get("max_lat") or ext.get("maxy") or ext.get("n")
        ls = ext.get("min_lat") or ext.get("miny") or ext.get("s")
        le = ext.get("max_lon") or ext.get("maxx") or ext.get("e")
        lw = ext.get("min_lon") or ext.get("minx") or ext.get("w")
        if None in (ln, ls, le, lw):
            continue
        # Bbox пересечение: не (card правее слоя) и не (card левее слоя) и т.д.
        if cw <= le and ce >= lw and cs <= ln and cn >= ls:
            kg.merge_rel(
                "InvestigationCard", "reg_number", reg_number,
                "SPATIALLY_COVERS",
                "Layer", "id", layer["layer_id"],
            )


# ---------------------------------------------------------------------------
# Data Cube блоки
# ---------------------------------------------------------------------------

def update_datacube_blocks(project_id: str, artifacts: dict, kg: Neo4jClient):
    """Добавить / обновить DatacubeBlock узлы из артефактов Data Cube.

    Args:
        project_id: ID проекта
        artifacts: словарь с данными из MinIO (scores, blocks, dominant_driver)
        kg: Neo4j клиент
    """
    blocks = artifacts.get("blocks", [])
    scores = {b.get("block_id"): b.get("score") for b in artifacts.get("scores", [])}
    drivers = {
        b.get("block_id"): {
            "dominant_driver": b.get("dominant_driver", ""),
            "dominant_driver_group": b.get("dominant_driver_group", ""),
        }
        for b in artifacts.get("dominant_drivers", [])
    }

    logger.info(
        "[KG] Индексирование DatacubeBlock: проект=%s blocks=%d scores=%d drivers=%d",
        project_id, len(blocks), len(scores), len(drivers),
    )
    blocks_without_score = [b.get("block_id") for b in blocks if b.get("block_id") not in scores]
    if blocks_without_score:
        logger.warning(
            "[KG] %d блоков без скора (первые 5: %s)",
            len(blocks_without_score), blocks_without_score[:5],
        )

    count = 0
    for block in blocks:
        block_id = block.get("block_id")
        if not block_id:
            continue
        driver_info = drivers.get(block_id, {})
        kg.merge_node("DatacubeBlock", {"block_id": block_id}, {
            "project_id": project_id,
            "score": float(scores.get(block_id) or 0),
            "lon": float(block.get("lon") or 0),
            "lat": float(block.get("lat") or 0),
            "dominant_driver": driver_info.get("dominant_driver", ""),
            "dominant_driver_group": driver_info.get("dominant_driver_group", ""),
        })
        kg.merge_rel("Project", "id", project_id, "HAS_BLOCK", "DatacubeBlock", "block_id", block_id)
        count += 1

    logger.info("[KG] Проект %s: %d DatacubeBlock узлов обновлено", project_id, count)


# ---------------------------------------------------------------------------
# Удаление проекта
# ---------------------------------------------------------------------------

_DELETE_PROJECT_CYPHER = """
    MATCH (p:Project {id: $pid})
    OPTIONAL MATCH (p)-[:HAS_GROUP]->(g:Group)
    OPTIONAL MATCH (p)-[:HAS_LAYER]->(l:Layer)
    OPTIONAL MATCH (l)-[:HAS_FIELD]->(f:Field)
    OPTIONAL MATCH (l)-[:HAS_TILE]->(t:SpatialTile)
    OPTIONAL MATCH (l)-[:HAS_ATTACHMENT]->(a:Attachment)
    OPTIONAL MATCH (a)-[:IS_CARD]->(c:InvestigationCard)
    OPTIONAL MATCH (p)-[:HAS_BLOCK]->(b:DatacubeBlock)
    DETACH DELETE p, g, l, f, t, a, c, b
"""


def delete_project_subgraph(project_id: str, kg: Neo4jClient):
    """Удалить все узлы проекта из KG.

    Mineral, Organization, WorkMethod — общие сущности, сохраняются.
    DETACH DELETE автоматически убирает все рёбра удалённых узлов.
    """
    kg.execute(_DELETE_PROJECT_CYPHER, {"pid": project_id})
    logger.info("[KG] Удалён проект %s из графа", project_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_binary(props: dict) -> bytes | None:
    """Попытаться извлечь бинарные данные из свойств fiona."""
    raw = props.get("DATA") or props.get("data")
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw)
        except Exception:
            return None
    return None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
