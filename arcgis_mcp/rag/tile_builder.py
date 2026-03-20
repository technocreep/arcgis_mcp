"""Построение пространственных тайлов для больших слоёв.

Делит extent слоя на регулярную сетку и вычисляет агрегированную
статистику по каждой ячейке (feature_count, доминирующие значения).
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

logger = logging.getLogger(__name__)

# Размер тайла по умолчанию (метры в проекции слоя)
DEFAULT_TILE_SIZE_M = 5000


def build_tiles(
    layer_id: str,
    project_id: str,
    gdb_path: str,
    extent_wgs84: dict,
    tile_size_deg: float | None = None,
) -> list[dict]:
    """Построить тайловую сетку для слоя.

    Args:
        layer_id: ID слоя в .gdb
        project_id: ID проекта
        gdb_path: путь к .gdb
        extent_wgs84: {minx, miny, maxx, maxy}
        tile_size_deg: размер тайла в градусах (по умолчанию ~0.05° ≈ 5км)

    Returns:
        Список dict с метаданными тайлов для загрузки в KG.
    """
    try:
        import geopandas as gpd
    except ImportError:
        logger.warning("geopandas не установлен, пропуск tile building")
        return []

    if not extent_wgs84:
        return []

    tile_size = tile_size_deg or 0.05  # ~5км на широте 60°

    minx = extent_wgs84.get("minx") or extent_wgs84.get("west") or extent_wgs84.get("w")
    miny = extent_wgs84.get("miny") or extent_wgs84.get("south") or extent_wgs84.get("s")
    maxx = extent_wgs84.get("maxx") or extent_wgs84.get("east") or extent_wgs84.get("e")
    maxy = extent_wgs84.get("maxy") or extent_wgs84.get("north") or extent_wgs84.get("n")

    if None in (minx, miny, maxx, maxy):
        return []

    # Читаем данные слоя
    try:
        gdf = gpd.read_file(gdb_path, layer=layer_id)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
    except Exception as e:
        logger.warning("Ошибка чтения слоя %s для тайлов: %s", layer_id, e)
        return []

    # Строим сетку тайлов
    cols = math.ceil((maxx - minx) / tile_size)
    rows = math.ceil((maxy - miny) / tile_size)
    cols = min(cols, 100)  # не более 100x100 тайлов
    rows = min(rows, 100)

    tiles = []
    for row in range(rows):
        for col in range(cols):
            tile_minx = minx + col * tile_size
            tile_miny = miny + row * tile_size
            tile_maxx = min(tile_minx + tile_size, maxx)
            tile_maxy = min(tile_miny + tile_size, maxy)

            bbox = {
                "w": round(tile_minx, 6), "s": round(tile_miny, 6),
                "e": round(tile_maxx, 6), "n": round(tile_maxy, 6),
            }

            # Фильтруем объекты в тайле
            try:
                from shapely.geometry import box
                tile_geom = box(tile_minx, tile_miny, tile_maxx, tile_maxy)
                mask = gdf.geometry.within(tile_geom) | gdf.geometry.intersects(tile_geom)
                tile_gdf = gdf[mask]
            except Exception:
                continue

            count = len(tile_gdf)
            if count == 0:
                continue

            # Доминирующие значения по строковым полям
            dominant: dict[str, Any] = {}
            for col_name in tile_gdf.columns:
                if col_name == "geometry":
                    continue
                if tile_gdf[col_name].dtype == object:
                    top = tile_gdf[col_name].value_counts().head(3)
                    if not top.empty:
                        dominant[col_name] = top.index[0]

            tile_id = f"{project_id}_{layer_id}_{row}_{col}"
            tiles.append({
                "id": tile_id,
                "layer_id": layer_id,
                "project_id": project_id,
                "bbox_json": json.dumps(bbox),
                "feature_count": count,
                "dominant_values_json": json.dumps(dominant, ensure_ascii=False),
                "stats_json": "{}",
            })

    logger.info("Построено %d тайлов для слоя %s", len(tiles), layer_id)
    return tiles
