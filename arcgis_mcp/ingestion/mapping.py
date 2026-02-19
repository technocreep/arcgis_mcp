"""Построение маппинга dataset_name ↔ display_name.

Центральный элемент ingestion-pipeline: сопоставляет технические имена слоёв
из .gdb с человекочитаемыми display_name из .aprx.

Иерархия источников (display_name_source):
    "aprx"      — Уровень 1: display_name из .aprx  (лучший)
    "dict"      — Уровень 2: имя из словаря общеупотребимых слоёв
    "inferred"  — Уровень 3: вывод по структуре данных (поля, геометрия)
    "gdb_only"  — Уровень 4: техническое имя, needs_review=True
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .parser_aprx import AprxData, LayerMapping
from .parser_gdb import FieldProfile, GdbData, LayerProfile


# ---------------------------------------------------------------------------
# Dataclasses результата
# ---------------------------------------------------------------------------

@dataclass
class MappedLayer:
    dataset_name: str
    display_name: str
    display_name_source: str          # "aprx" | "dict" | "inferred" | "gdb_only"
    group: str | None
    feature_dataset: str | None
    units: str | None
    description: str | None
    aprx_file: str | None
    aprx_visibility: bool | None
    aprx_display_field: str | None
    aprx_label_expression: str | None
    field_aliases: dict[str, str]
    needs_review: bool = False        # True для Уровня 4


@dataclass
class UnmappedLayer:
    dataset_name: str
    reason: str
    display_name: str
    display_name_source: str = "gdb_only"
    needs_review: bool = True


@dataclass
class MappingQuality:
    total_gdb_layers: int
    mapped_from_aprx: int
    mapped_from_dict: int
    mapped_from_inferred: int
    unmapped: int
    coverage_percent: float           # только aprx
    has_groups: bool
    groups_count: int


@dataclass
class LayerMappingResult:
    mapped: list[MappedLayer]
    unmapped: list[UnmappedLayer]
    quality: MappingQuality
    warnings: list[str]

    # Быстрый доступ: dataset_name → MappedLayer
    _index: dict[str, MappedLayer] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        self._index = {m.dataset_name: m for m in self.mapped}

    def get(self, dataset_name: str) -> MappedLayer | None:
        return self._index.get(dataset_name)


# ---------------------------------------------------------------------------
# Уровень 2: словарь общеупотребимых GIS-названий
# ---------------------------------------------------------------------------

KNOWN_LAYERS: dict[str, str] = {
    # Топооснова
    "river":       "Реки",
    "rivers":      "Реки",
    "lake":        "Озёра",
    "lakes":       "Озёра",
    "road":        "Дороги",
    "roads":       "Дороги",
    "town":        "Населённые пункты",
    "towns":       "Населённые пункты",
    "settlement":  "Населённые пункты",
    "relief":      "Рельеф (горизонтали)",
    "contours":    "Горизонтали рельефа",
    "obl_p":       "Административные границы (область)",
    "region":      "Административные границы",
    # Рамки и разграфка
    "rama":        "Рамка карты",
    "ramka":       "Рамка листа",
    "frame":       "Рамка карты",
    "grid":        "Сетка координат",
    "gridsheet":   "Номенклатурные листы",
    # Лицензионные участки
    "licences":    "Лицензионные участки",
    "license":     "Лицензионный участок",
    "licence":     "Лицензионный участок",
    # Геологические данные
    "geology":     "Геология",
    "faults":      "Разломы",
    "fault":       "Разлом",
    "lineament":   "Линеаменты",
    "lin":         "Линеаменты",
    "contacts":    "Геологические контакты",
    # Скважины / опробование
    "wells":       "Скважины",
    "boreholes":   "Скважины",
    "samples":     "Пробы",
    "trenches":    "Канавы",
    # Изученность
    "izuch":       "Изученность",
    "survey":      "Результаты съёмки",
    # Прочее
    "border":      "Граница",
    "boundary":    "Граница",
    "annotation":  "Аннотации",
}


def _lookup_known_layer(dataset_name: str) -> str | None:
    """Поиск display_name в словаре по dataset_name (case-insensitive)."""
    key = dataset_name.lower().strip("_")
    return KNOWN_LAYERS.get(key)


# ---------------------------------------------------------------------------
# Уровень 3: вывод по структуре данных
# ---------------------------------------------------------------------------

# Наборы полей — характерные для определённых типов данных
_FIELD_SIGNATURES: list[tuple[frozenset[str], str]] = [
    # Рельеф
    (frozenset({"phlr_abs", "cont", "height", "altitude"}), "Рельеф (горизонтали)"),
    # Гравика
    (frozenset({"gms", "grav", "delta_g", "dg"}), "Гравиметрические данные"),
    # Магнитка
    (frozenset({"mms", "mag", "delta_t", "dt", "ntl"}), "Магниторазведочные данные"),
    # Геохимия
    (frozenset({"au", "cu", "pb", "zn", "ag"}), "Геохимические пробы"),
    # Скважины
    (frozenset({"depth", "azimuth", "dip", "collar"}), "Скважины"),
    # Административные границы
    (frozenset({"okrug", "region", "oblast", "district"}), "Административные границы"),
]

_GEOM_HINTS: dict[str, str] = {
    # Геотип → суффикс к выведенному имени
    "Point":          "точечный",
    "MultiPoint":     "точечный",
    "LineString":     "линейный",
    "MultiLineString": "линейный",
    "Polygon":        "полигональный",
    "MultiPolygon":   "полигональный",
    "3D MultiPolygon": "3D полигональный",
    "3D MultiPoint":  "3D точечный",
}

_DATASET_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"_r_?\d{2}$", re.IGNORECASE), "лист номенклатуры"),
    (re.compile(r"^izol", re.IGNORECASE),       "Изолинии"),
    (re.compile(r"^extr",  re.IGNORECASE),      "Экстремумы"),
    (re.compile(r"^iz_?uch", re.IGNORECASE),    "Изученность"),
    (re.compile(r"_?attach$", re.IGNORECASE),   "Таблица вложений"),
    (re.compile(r"^gr_?iz", re.IGNORECASE),     "Гравиметрическая изученность"),
    (re.compile(r"^mag_?iz", re.IGNORECASE),    "Аэромагнитная изученность"),
    (re.compile(r"^opmar",  re.IGNORECASE),     "Оперативный маршрут"),
    (re.compile(r"n_pole",  re.IGNORECASE),     "Нормальное поле"),
]


def infer_layer_meaning(
    layer_id: str,
    fields: list[FieldProfile],
    geometry_type: str | None,
    feature_count: int,
) -> str | None:
    """Попытаться вывести display_name по структуре данных.

    Returns:
        Выведенное название или None если не удалось определить.
    """
    field_names_lower = {f.name.lower() for f in fields}

    # --- По характерным наборам полей ---
    for signature, label in _FIELD_SIGNATURES:
        if signature & field_names_lower:   # пересечение множеств
            return label

    # --- По паттерну dataset_name ---
    for pattern, label in _DATASET_HINTS:
        if pattern.search(layer_id):
            return label

    # --- По суффиксу dataset_name + геометрии ---
    # Попытка сформировать осмысленное имя из частей dataset_name
    # Например: "BaseA_R_42" → видим "_R_42" → "геологическая модель лист R-42"
    geom_hint = _GEOM_HINTS.get(geometry_type or "", "")

    # Если имя содержит CamelCase или подчёркивания — попытка красиво разбить
    readable = _humanize_dataset_name(layer_id)
    if readable and readable != layer_id:
        return readable

    return None


def _humanize_dataset_name(name: str) -> str | None:
    """Попытаться сделать имя слоя читабельным.

    Примеры:
        "BaseA_R_42"   → "BaseA R 42"
        "DplcL_R_42"   → "DplcL R 42"
        "Скважины_ГСК" → "Скважины ГСК"  (уже читабельно — оставить)
    """
    # Если имя уже содержит кириллицу — оставить как есть
    if re.search(r"[а-яёА-ЯЁ]", name):
        return name

    # Убираем подчёркивания, разбиваем CamelCase
    s = name.replace("_", " ").strip()
    # CamelCase → слова
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Если результат слишком короткий или бессмысленный — не возвращаем
    if len(s) <= 3 or s == name.replace("_", " "):
        return None
    return s


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def build_mapping(aprx_data: AprxData | None, gdb_data: GdbData) -> LayerMappingResult:
    """Сопоставить слои .gdb с display_name, используя 4-уровневую иерархию.

    Уровни:
        1. display_name из .aprx          → source="aprx"
        2. Словарь KNOWN_LAYERS           → source="dict"
        3. Вывод по полям/геометрии       → source="inferred"
        4. Техническое имя (needs_review) → source="gdb_only"

    Args:
        aprx_data: результат parse_aprx() или None
        gdb_data:  результат parse_gdb()
    """
    warnings_list: list[str] = []

    # Строим индекс aprx по dataset_name
    aprx_index: dict[str, LayerMapping] = {}
    if aprx_data is not None:
        for lm in aprx_data.layer_mappings:
            aprx_index[lm.dataset_name] = lm
            aprx_index.setdefault(lm.dataset_name.lower(), lm)

    # Фильтруем слои — исключаем attachment-таблицы
    gdb_layers = [lp for lp in gdb_data.layers if not lp.is_attachment_table]

    mapped: list[MappedLayer] = []
    unmapped: list[UnmappedLayer] = []

    cnt_aprx = cnt_dict = cnt_inferred = cnt_gdb_only = 0

    for lp in gdb_layers:
        ds_name = lp.layer_id

        # --- Уровень 1: .aprx ---
        lm = aprx_index.get(ds_name) or aprx_index.get(ds_name.lower())
        if lm is not None:
            mapped.append(MappedLayer(
                dataset_name=ds_name,
                display_name=lm.display_name,
                display_name_source="aprx",
                group=lm.group,
                feature_dataset=lm.feature_dataset,
                units=lm.units,
                description=lm.description,
                aprx_file=lm.aprx_file,
                aprx_visibility=lm.visibility,
                aprx_display_field=lm.display_field,
                aprx_label_expression=lm.label_expression,
                field_aliases=lm.field_aliases,
                needs_review=False,
            ))
            cnt_aprx += 1
            continue

        # --- Уровень 2: словарь ---
        dict_name = _lookup_known_layer(ds_name)
        if dict_name is not None:
            mapped.append(MappedLayer(
                dataset_name=ds_name,
                display_name=dict_name,
                display_name_source="dict",
                group=None,
                feature_dataset=None,
                units=None,
                description=None,
                aprx_file=None,
                aprx_visibility=None,
                aprx_display_field=None,
                aprx_label_expression=None,
                field_aliases={},
                needs_review=False,
            ))
            cnt_dict += 1
            continue

        # --- Уровень 3: вывод по данным ---
        inferred_name = infer_layer_meaning(
            ds_name, lp.fields, lp.geometry_type, lp.feature_count
        )
        if inferred_name is not None:
            warnings_list.append(
                f"Слой '{ds_name}': display_name выведен автоматически → \"{inferred_name}\""
            )
            mapped.append(MappedLayer(
                dataset_name=ds_name,
                display_name=inferred_name,
                display_name_source="inferred",
                group=None,
                feature_dataset=None,
                units=None,
                description=None,
                aprx_file=None,
                aprx_visibility=None,
                aprx_display_field=None,
                aprx_label_expression=None,
                field_aliases={},
                needs_review=False,
            ))
            cnt_inferred += 1
            continue

        # --- Уровень 4: gdb_only, needs_review ---
        warnings_list.append(
            f"Слой '{ds_name}' не найден в .aprx и не распознан автоматически — требует ручной разметки"
        )
        unmapped.append(UnmappedLayer(
            dataset_name=ds_name,
            reason="Отсутствует в .aprx, не распознан по словарю и структуре данных",
            display_name=ds_name,
            display_name_source="gdb_only",
            needs_review=True,
        ))
        # Добавляем и в mapped (с флагом) — агент должен видеть все слои
        mapped.append(MappedLayer(
            dataset_name=ds_name,
            display_name=ds_name,
            display_name_source="gdb_only",
            group=None,
            feature_dataset=None,
            units=None,
            description=None,
            aprx_file=None,
            aprx_visibility=None,
            aprx_display_field=None,
            aprx_label_expression=None,
            field_aliases={},
            needs_review=True,
        ))
        cnt_gdb_only += 1

    # --- Режим без .aprx ---
    if aprx_data is None:
        warnings_list.insert(0, (
            "Загрузка без .aprx. Названия слоёв будут техническими, "
            "маппинг display_name невозможен. Рекомендуется загрузить .aprx."
        ))

    total = len(gdb_layers)
    coverage = round(cnt_aprx / total * 100, 1) if total > 0 else 0.0
    groups_count = len(aprx_data.groups) if aprx_data else 0

    quality = MappingQuality(
        total_gdb_layers=total,
        mapped_from_aprx=cnt_aprx,
        mapped_from_dict=cnt_dict,
        mapped_from_inferred=cnt_inferred,
        unmapped=cnt_gdb_only,
        coverage_percent=coverage,
        has_groups=groups_count > 0,
        groups_count=groups_count,
    )

    return LayerMappingResult(
        mapped=mapped,
        unmapped=unmapped,
        quality=quality,
        warnings=warnings_list,
    )
