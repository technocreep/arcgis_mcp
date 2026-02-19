"""Парсер ArcGIS Pro Project (.aprx).

.aprx — ZIP-архив, содержащий JSON-файлы в формате CIM (Cartographic Information Model).
Извлекаем: display_name, dataset_name, feature_dataset, groups, units, datum_transforms.
"""

import json
import re
import zipfile
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class LayerMapping:
    dataset_name: str            # имя датасета в .gdb (например "gms_r")
    display_name: str            # человекочитаемое из .aprx (например "Поле дельта G (мГал)")
    group: str | None            # группа слоёв (например "Гравика R-42")
    feature_dataset: str | None  # feature dataset в .gdb (Study, Licences, Grid…)
    units: str | None            # единицы измерения, извлечённые из display_name
    description: str | None
    visibility: bool
    display_field: str | None
    label_expression: str | None
    field_aliases: dict[str, str]  # {field_name: alias}
    layer_type: str | None         # "Operational" / "BasemapBackground"
    aprx_file: str               # путь к JSON внутри .aprx (например "Map/gms_r.json")


@dataclass
class AprxData:
    map_name: str
    layer_mappings: list[LayerMapping]
    groups: dict[str, list[str]]   # group_name → [dataset_names]
    datum_transforms: list[str]
    basemaps: list[str]
    layer_order: list[str]         # dataset_names в порядке отображения (сверху вниз)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

_UNITS_PATTERN = re.compile(
    r'\(([^)]*(?:мГал|нТл(?:/км)?|нТл|Э|км|x100\s*нТл)[^)]*)\)',
    re.IGNORECASE,
)


def _extract_units(display_name: str) -> str | None:
    """Извлечь единицы измерения из display_name вида 'Поле дельта G (мГал)'."""
    m = _UNITS_PATTERN.search(display_name)
    return m.group(1).strip() if m else None


def _decode_name(raw: str) -> str:
    """Декодировать Unicode-escape последовательности в строке JSON.

    В .aprx поля name содержат строки вида \\uXXXX — Python их декодирует
    автоматически при json.loads(), но на всякий случай оставляем fallback.
    """
    try:
        return raw.encode("utf-8").decode("unicode_escape").encode("latin-1").decode("utf-8")
    except Exception:
        return raw


def _safe_name(raw: str) -> str:
    """Вернуть человекочитаемое имя: если содержит нормальный текст — вернуть как есть."""
    # json.loads уже декодировал \uXXXX → unicode, поэтому обычно raw уже корректен.
    return raw.strip()


def _get_nested(obj: dict, *keys, default=None):
    """Безопасный доступ к вложенным ключам словаря."""
    for k in keys:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(k, default)
        if obj is None:
            return default
    return obj


# ---------------------------------------------------------------------------
# Разрезолвинг CIMPATH → dataset_name
# ---------------------------------------------------------------------------

def _cimpath_to_filename(cimpath: str) -> str | None:
    """Из 'CIMPATH=Map/gms_r.json' извлечь 'Map/gms_r.json'."""
    if cimpath.startswith("CIMPATH="):
        return cimpath[len("CIMPATH="):]
    return None


# ---------------------------------------------------------------------------
# Парсинг отдельного JSON-файла слоя
# ---------------------------------------------------------------------------

def _parse_feature_layer_json(data: dict, aprx_file: str) -> LayerMapping | None:
    """Из JSON CIMFeatureLayer/CIMRasterLayer собрать LayerMapping.

    Возвращает None, если это не операционный слой с данными из .gdb.
    """
    layer_type_cim = data.get("type", "")

    # Интересуют только CIMFeatureLayer и CIMRasterLayer
    if layer_type_cim not in (
        "CIMFeatureLayer",
        "CIMRasterLayer",
        "CIMAGSSubLayer",
        "CIMStandaloneTable",
    ):
        return None

    raw_name = data.get("name", "")
    if not raw_name:
        return None
    display_name = _safe_name(raw_name)

    # dataConnection
    data_conn = data.get("featureTable", {}).get("dataConnection", {}) or \
                data.get("dataConnection", {})
    dataset_name: str = data_conn.get("dataset", "") or ""
    feature_dataset: str | None = data_conn.get("featureDataset") or None

    if not dataset_name:
        return None

    # description
    description: str | None = data.get("description") or None

    # visibility
    visibility: bool = bool(data.get("visibility", True))

    # layer_type (Operational / BasemapBackground)
    layer_type: str | None = data.get("layerType") or None

    # display_field
    display_field: str | None = _get_nested(data, "featureTable", "displayField") or \
                                data.get("displayField") or None

    # label_expression из первого LabelClass
    label_expr: str | None = None
    label_classes = data.get("labelClasses", []) or []
    if label_classes:
        label_expr = label_classes[0].get("expression") or None

    # field_aliases из fieldDescriptions
    field_aliases: dict[str, str] = {}
    field_descs = _get_nested(data, "featureTable", "fieldDescriptions") or []
    for fd in field_descs:
        fname = fd.get("fieldName")
        alias = fd.get("alias")
        if fname and alias and alias != fname:
            field_aliases[fname] = alias

    units = _extract_units(display_name)

    return LayerMapping(
        dataset_name=dataset_name,
        display_name=display_name,
        group=None,          # будет проставлено при парсинге групп
        feature_dataset=feature_dataset,
        units=units,
        description=description,
        visibility=visibility,
        display_field=display_field,
        label_expression=label_expr,
        field_aliases=field_aliases,
        layer_type=layer_type,
        aprx_file=aprx_file,
    )


# ---------------------------------------------------------------------------
# Основной парсер
# ---------------------------------------------------------------------------

def parse_aprx(aprx_path: str | Path) -> AprxData:
    """Распарсить .aprx и вернуть AprxData.

    Args:
        aprx_path: путь к .aprx файлу (ZIP-архиву)

    Returns:
        AprxData с маппингом слоёв, группами, datum transforms и т.д.

    Raises:
        ValueError: если файл не является валидным .aprx
        FileNotFoundError: если файл не существует
    """
    aprx_path = Path(aprx_path)
    if not aprx_path.exists():
        raise FileNotFoundError(f"Файл не найден: {aprx_path}")

    if not zipfile.is_zipfile(aprx_path):
        raise ValueError(f"Файл не является ZIP-архивом (.aprx): {aprx_path}")

    # dataset_name → LayerMapping (промежуточный, без groups)
    mappings_by_dataset: dict[str, LayerMapping] = {}
    # aprx_file → LayerMapping
    mappings_by_aprx_file: dict[str, LayerMapping] = {}

    groups: dict[str, list[str]] = {}
    datum_transforms: list[str] = []
    basemaps: list[str] = []
    map_name: str = ""
    layer_order: list[str] = []

    with zipfile.ZipFile(aprx_path, "r") as zf:
        namelist = zf.namelist()

        # --- Шаг 1: Парсим ВСЕ JSON в архиве по содержимому, а не по имени файла.
        #
        # Проблема: ArcGIS Pro иногда сохраняет слои с побитыми именами файлов
        # (например "________________/____________.json"), поэтому фильтрация по
        # паттерну "Map/*.json" пропускала такие слои.
        # Решение: читаем каждый .json, смотрим на поле "type" внутри.
        #
        # Пропускаем известные тяжёлые файлы, которые не содержат слоёв:
        _SKIP_FILES = {"gisproject.json", "index.json"}
        # Лимит размера: файлы >2MB не могут быть описанием одного слоя
        _MAX_LAYER_JSON_BYTES = 2 * 1024 * 1024

        all_json_files = [n for n in namelist if n.lower().endswith(".json")]

        group_layer_data: list[tuple[str, dict]] = []   # (aprx_file, json_data) для групп

        for map_file in all_json_files:
            # Пропускаем известные нелоевые файлы
            basename = map_file.split("/")[-1].lower()
            if basename in _SKIP_FILES:
                continue

            # Пропускаем слишком большие файлы (не могут быть описанием одного слоя)
            try:
                info = zf.getinfo(map_file)
                if info.file_size > _MAX_LAYER_JSON_BYTES:
                    continue
            except KeyError:
                continue

            try:
                raw = zf.read(map_file).decode("utf-8", errors="replace")
                data = json.loads(raw)
            except (json.JSONDecodeError, Exception):
                continue

            cim_type = data.get("type", "")

            if cim_type == "CIMGroupLayer":
                group_layer_data.append((map_file, data))
                continue

            if cim_type == "CIMMap":
                # map.json обрабатываем отдельно ниже
                continue

            lm = _parse_feature_layer_json(data, map_file)
            if lm is not None:
                # dataset_name — главный ключ маппинга; первый найденный выигрывает
                mappings_by_dataset.setdefault(lm.dataset_name, lm)
                mappings_by_aprx_file[map_file] = lm

        # --- Шаг 2: Разрезолвить группы ---
        for aprx_file, gdata in group_layer_data:
            raw_gname = gdata.get("name", "")
            if not raw_gname:
                continue
            group_name = _safe_name(raw_gname)

            member_dataset_names: list[str] = []
            layers_refs = gdata.get("layers", []) or []
            for ref in layers_refs:
                member_file = _cimpath_to_filename(ref) if isinstance(ref, str) else None
                if member_file is None:
                    continue
                lm = mappings_by_aprx_file.get(member_file)
                if lm:
                    lm.group = group_name
                    member_dataset_names.append(lm.dataset_name)

            if member_dataset_names:
                groups[group_name] = member_dataset_names

        # --- Шаг 3: Парсим map/map.json ---
        map_json_candidates = [
            n for n in namelist
            if re.match(r"(?i)^map/map\.json$", n)
        ]
        for map_json_path in map_json_candidates:
            try:
                raw = zf.read(map_json_path).decode("utf-8", errors="replace")
                mdata = json.loads(raw)
            except Exception:
                continue

            if mdata.get("type") != "CIMMap":
                continue

            raw_map_name = mdata.get("name", "")
            map_name = _safe_name(raw_map_name) if raw_map_name else ""

            # datum transforms
            for dt in mdata.get("datumTransforms", []) or []:
                if isinstance(dt, dict):
                    gt = dt.get("geoTransforms", []) or []
                    for g in gt:
                        if isinstance(g, dict):
                            wkt_name = _get_nested(g, "geoTransformation", "name") or \
                                       _get_nested(g, "name")
                            if wkt_name:
                                datum_transforms.append(wkt_name)
                elif isinstance(dt, str):
                    datum_transforms.append(dt)

            # layer_order — порядок слоёв в карте (CIMPATH → dataset_name)
            for ref in mdata.get("layers", []) or []:
                member_file = _cimpath_to_filename(ref) if isinstance(ref, str) else None
                if member_file is None:
                    continue
                lm = mappings_by_aprx_file.get(member_file)
                if lm:
                    layer_order.append(lm.dataset_name)

        # --- Шаг 4: Basemaps из GISProject.json или layout ---
        gis_project_candidates = [
            n for n in namelist
            if n.lower() == "gisproject.json"
        ]
        for gp_path in gis_project_candidates:
            try:
                raw = zf.read(gp_path).decode("utf-8", errors="replace")
                gp = json.loads(raw)
                # ищем basemapLayers рекурсивно — слишком большой файл, пропускаем deep search
                # берём только верхний уровень
                for item in gp.get("basemaps", []) or []:
                    bname = item.get("name") or item.get("mapServiceLayer", {}).get("url", "")
                    if bname:
                        basemaps.append(bname)
            except Exception:
                pass

    return AprxData(
        map_name=map_name,
        layer_mappings=list(mappings_by_dataset.values()),
        groups=groups,
        datum_transforms=list(dict.fromkeys(datum_transforms)),   # дедупликация
        basemaps=basemaps,
        layer_order=layer_order,
    )
