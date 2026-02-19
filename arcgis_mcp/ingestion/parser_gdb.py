"""Парсер Esri File Geodatabase (.gdb).

Использует fiona для чтения метаданных и geopandas для статистики атрибутов.
Для слоёв > GDB_LARGE_LAYER_THRESHOLD объектов — только schema + count + extent.
Таблицы вложений (*__ATTACH) определяются отдельно.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fiona
import geopandas as gpd
import numpy as np
from pyproj import CRS, Transformer

from config import GDB_LARGE_LAYER_THRESHOLD, GDB_STATS_TOP_VALUES_LIMIT

warnings.filterwarnings("ignore", category=UserWarning)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class FieldProfile:
    name: str
    dtype: str
    nulls: int
    # числовые поля
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None
    # категориальные / строковые
    unique_count: int | None = None
    top_values: dict[str, int] | None = None


@dataclass
class LayerProfile:
    layer_id: str
    geometry_type: str | None          # Point, MultiPolygon, …, None (таблица)
    feature_count: int
    crs_epsg: int | None
    crs_wkt: str | None
    extent_native: dict | None         # {minx, miny, maxx, maxy}
    extent_wgs84: dict | None          # {min_lon, min_lat, max_lon, max_lat}
    fields: list[FieldProfile]
    is_attachment_table: bool
    is_large: bool                     # >GDB_LARGE_LAYER_THRESHOLD — без полной статистики


@dataclass
class AttachmentRecord:
    index: int
    att_name: str
    content_type: str
    data_size: int
    rel_globalid: str | None
    has_data: bool


@dataclass
class AttachmentTable:
    table_name: str
    parent_layer: str | None           # имя связанного слоя
    total_attachments: int
    attachments: list[AttachmentRecord]


@dataclass
class GdbData:
    layers: list[LayerProfile]
    attachment_tables: list[AttachmentTable]


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _fiona_dtype_to_str(ftype: str) -> str:
    """Нормализовать fiona-тип поля к читаемой строке."""
    mapping = {
        "int": "int32",
        "int32": "int32",
        "int64": "int64",
        "float": "float64",
        "float32": "float32",
        "float64": "float64",
        "str": "str",
        "date": "datetime",
        "datetime": "datetime",
        "time": "time",
        "bytes": "bytes",
    }
    return mapping.get(ftype.lower(), ftype)


def _epsg_from_crs(crs_dict: dict | None, crs_wkt: str | None) -> int | None:
    """Попытаться извлечь код EPSG."""
    if crs_dict is None and crs_wkt is None:
        return None
    try:
        if crs_wkt:
            proj_crs = CRS.from_wkt(crs_wkt)
        else:
            proj_crs = CRS.from_dict(crs_dict)
        epsg = proj_crs.to_epsg()
        return epsg
    except Exception:
        return None


def _transform_extent_to_wgs84(
    minx: float, miny: float, maxx: float, maxy: float, crs_wkt: str | None
) -> dict | None:
    """Перевести bbox в WGS84 {min_lon, min_lat, max_lon, max_lat}."""
    if crs_wkt is None:
        return None
    try:
        src_crs = CRS.from_wkt(crs_wkt)
        transformer = Transformer.from_crs(src_crs, CRS.from_epsg(4326), always_xy=True)
        lon_min, lat_min = transformer.transform(minx, miny)
        lon_max, lat_max = transformer.transform(maxx, maxy)
        return {
            "min_lon": round(lon_min, 6),
            "min_lat": round(lat_min, 6),
            "max_lon": round(lon_max, 6),
            "max_lat": round(lat_max, 6),
        }
    except Exception:
        return None


def _is_numeric_dtype(dtype: str) -> bool:
    return dtype in ("int32", "int64", "float32", "float64", "int", "float")


# ---------------------------------------------------------------------------
# Статистика по полям (для слоёв < GDB_LARGE_LAYER_THRESHOLD)
# ---------------------------------------------------------------------------

def _compute_field_stats(gdf: gpd.GeoDataFrame, schema_fields: dict) -> list[FieldProfile]:
    """Вычислить статистику по полям GeoDataFrame."""
    profiles: list[FieldProfile] = []

    for col in gdf.columns:
        if col == "geometry":
            continue

        ftype_raw = schema_fields.get(col, str(gdf[col].dtype))
        dtype = _fiona_dtype_to_str(str(ftype_raw))

        series = gdf[col]
        nulls = int(series.isna().sum())

        if _is_numeric_dtype(dtype) or np.issubdtype(series.dtype, np.number):
            valid = series.dropna()
            fp = FieldProfile(
                name=col,
                dtype=dtype,
                nulls=nulls,
                min=float(valid.min()) if len(valid) > 0 else None,
                max=float(valid.max()) if len(valid) > 0 else None,
                mean=float(valid.mean()) if len(valid) > 0 else None,
                std=float(valid.std()) if len(valid) > 0 else None,
            )
        else:
            valid = series.dropna().astype(str)
            unique_count = int(valid.nunique())
            top = valid.value_counts().head(GDB_STATS_TOP_VALUES_LIMIT).to_dict()
            fp = FieldProfile(
                name=col,
                dtype=dtype,
                nulls=nulls,
                unique_count=unique_count,
                top_values={str(k): int(v) for k, v in top.items()} if top else None,
            )
        profiles.append(fp)

    return profiles


def _schema_fields_only(schema_fields: dict) -> list[FieldProfile]:
    """Создать профили полей только из схемы (без статистики, для больших слоёв)."""
    profiles: list[FieldProfile] = []
    for fname, ftype in schema_fields.items():
        profiles.append(FieldProfile(
            name=fname,
            dtype=_fiona_dtype_to_str(str(ftype)),
            nulls=0,
        ))
    return profiles


# ---------------------------------------------------------------------------
# Парсинг таблицы вложений
# ---------------------------------------------------------------------------

def _parse_attachment_table(gdb_path: str, table_name: str) -> AttachmentTable:
    """Прочитать таблицу вложений *__ATTACH."""
    parent_layer = table_name.replace("__ATTACH", "")
    records: list[AttachmentRecord] = []

    try:
        with fiona.open(gdb_path, layer=table_name) as src:
            for i, feat in enumerate(src):
                props = dict(feat.get("properties") or {})
                records.append(AttachmentRecord(
                    index=i,
                    att_name=str(props.get("ATT_NAME") or props.get("att_name") or ""),
                    content_type=str(props.get("CONTENT_TYPE") or props.get("content_type") or ""),
                    data_size=int(props.get("DATA_SIZE") or props.get("data_size") or 0),
                    rel_globalid=str(props.get("REL_GLOBALID") or props.get("rel_globalid") or "") or None,
                    has_data="DATA" in props or "data" in props,
                ))
    except Exception:
        pass

    return AttachmentTable(
        table_name=table_name,
        parent_layer=parent_layer,
        total_attachments=len(records),
        attachments=records,
    )


# ---------------------------------------------------------------------------
# Основной парсер
# ---------------------------------------------------------------------------

def parse_gdb(gdb_path: str | Path) -> GdbData:
    """Прочитать .gdb и вернуть GdbData.

    Args:
        gdb_path: путь к директории .gdb

    Returns:
        GdbData со списком LayerProfile и AttachmentTable

    Raises:
        FileNotFoundError: если директория не существует
        ValueError: если fiona не может прочитать файл
    """
    gdb_path = Path(gdb_path)
    if not gdb_path.exists():
        raise FileNotFoundError(f"Не найдена директория: {gdb_path}")

    gdb_path_str = str(gdb_path)

    try:
        all_layers = fiona.listlayers(gdb_path_str)
    except Exception as e:
        raise ValueError(f"Не удалось открыть .gdb: {e}") from e

    layers: list[LayerProfile] = []
    attachment_tables_raw: list[str] = []

    for layer_name in all_layers:
        # Определить — это таблица вложений?
        if layer_name.endswith("__ATTACH"):
            attachment_tables_raw.append(layer_name)
            continue

        try:
            with fiona.open(gdb_path_str, layer=layer_name) as src:
                schema = src.schema
                feature_count = len(src)
                crs_dict = src.crs
                crs_wkt = src.crs_wkt if hasattr(src, "crs_wkt") else None

                # CRS
                epsg = _epsg_from_crs(crs_dict, crs_wkt)
                if crs_wkt is None and crs_dict:
                    try:
                        crs_wkt = CRS.from_dict(crs_dict).to_wkt()
                    except Exception:
                        pass

                # Geometry type
                geom_type: str | None = schema.get("geometry") or None
                if geom_type and geom_type.lower() in ("none", "null", ""):
                    geom_type = None

                # Extent
                bounds = src.bounds   # (minx, miny, maxx, maxy)
                extent_native: dict | None = None
                extent_wgs84: dict | None = None
                if bounds and any(b != 0 for b in bounds):
                    extent_native = {
                        "minx": bounds[0], "miny": bounds[1],
                        "maxx": bounds[2], "maxy": bounds[3],
                    }
                    if crs_wkt:
                        extent_wgs84 = _transform_extent_to_wgs84(
                            bounds[0], bounds[1], bounds[2], bounds[3], crs_wkt
                        )

                schema_fields: dict = dict(schema.get("properties", {}))

                is_large = feature_count > GDB_LARGE_LAYER_THRESHOLD

                if is_large or feature_count == 0:
                    # Только schema без полной статистики
                    field_profiles = _schema_fields_only(schema_fields)
                else:
                    # Загружаем данные для статистики
                    try:
                        gdf = gpd.read_file(gdb_path_str, layer=layer_name)
                        field_profiles = _compute_field_stats(gdf, schema_fields)
                    except Exception:
                        field_profiles = _schema_fields_only(schema_fields)

        except Exception as e:
            # Если слой не читается — создаём минимальный профиль
            layers.append(LayerProfile(
                layer_id=layer_name,
                geometry_type=None,
                feature_count=0,
                crs_epsg=None,
                crs_wkt=None,
                extent_native=None,
                extent_wgs84=None,
                fields=[],
                is_attachment_table=False,
                is_large=False,
            ))
            continue

        layers.append(LayerProfile(
            layer_id=layer_name,
            geometry_type=geom_type,
            feature_count=feature_count,
            crs_epsg=epsg,
            crs_wkt=crs_wkt,
            extent_native=extent_native,
            extent_wgs84=extent_wgs84,
            fields=field_profiles,
            is_attachment_table=False,
            is_large=is_large,
        ))

    # Парсим таблицы вложений
    attachment_tables: list[AttachmentTable] = []
    for tname in attachment_tables_raw:
        at = _parse_attachment_table(gdb_path_str, tname)
        attachment_tables.append(at)

        # Также добавляем как LayerProfile с is_attachment_table=True
        layers.append(LayerProfile(
            layer_id=tname,
            geometry_type=None,
            feature_count=at.total_attachments,
            crs_epsg=None,
            crs_wkt=None,
            extent_native=None,
            extent_wgs84=None,
            fields=[],
            is_attachment_table=True,
            is_large=False,
        ))

    return GdbData(layers=layers, attachment_tables=attachment_tables)
