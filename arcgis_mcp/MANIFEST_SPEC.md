# GIS Manifest — спецификация формата

## Обзор

Manifest — машиночитаемый JSON-файл, фиксирующий "истину проекта".
Формируется автоматически при загрузке проекта через ingestion pipeline.
Служит основным источником для ответов агента на справочные и инвентаризационные запросы
(~80% обращений) без необходимости прямого чтения .gdb.

---

## Источники данных при загрузке

При загрузке проекта пользователь предоставляет файлы:

| Файл | Формат | Обязательный | Что даёт |
|------|--------|:---:|----------|
| `*.gdb` | Esri File Geodatabase (папка) | ✅ | Слои, атрибуты, геометрия, вложения, CRS — основной источник данных |
| `*.aprx` | ArcGIS Pro Project (zip → JSON) | ✅ | Display names, группировка слоёв, единицы измерения, feature datasets, маппинг dataset→display |
| `*.atbx` | ArcGIS Toolbox (7z → JSON + .py) | ❌ | Описания инструментов геообработки, параметры, Python-скрипты |

> **Почему .aprx обязательный:** Названия слоёв в .gdb часто непрозрачны (например `gms_r`,
> `fhg_gr`, `mms_r`). Только из .aprx можно извлечь человекочитаемые display names с единицами
> измерения ("Поле дельта G (мГал)", "Полный гориз. градиент дельта T (нТл/км)").
> Без этого маппинга агент не сможет корректно интерпретировать данные — путаница между
> гравикой и магниткой на примере Лекын-Тальбейского проекта это подтвердила.
> При загрузке без .aprx система должна выдавать предупреждение и помечать все слои
> как `"display_name_source": "gdb_only"` с quality score penalty.

---

## Что извлекаем из каждого источника

### 1. File Geodatabase (.gdb)

**Инструмент:** `fiona.listlayers()` + `fiona.open()` + `geopandas.read_file()`

**Для каждого слоя:**

```
layer_id              — имя датасета в .gdb (например "gms_r", "Скважины_ГСК")
geometry_type         — тип геометрии: Point, MultiPolygon, MultiLineString, None (таблица), 3D MultiPolygon
feature_count         — количество объектов
crs_epsg              — код EPSG (например 7683 = ГСК-2011)
crs_wkt               — полный WKT строки CRS
extent_native         — bbox в исходной CRS {minx, miny, maxx, maxy}
extent_wgs84          — bbox в WGS84 {min_lon, min_lat, max_lon, max_lat}
fields[]              — массив полей:
  ├── name            — имя поля
  ├── dtype           — тип данных (str, int32, float64, datetime)
  ├── nulls           — количество NULL-значений
  ├── (числовые)      — min, max, mean, std
  └── (категориальные)— unique_count, top_values (до 20 значений с частотами)
```

**Для таблиц вложений (например `Izuch_A_sel__ATTACH`):**

```
attachment_table      — имя таблицы
parent_layer          — связанный слой (определяется по REL_GLOBALID)
total_attachments     — количество вложений
attachments[]         — массив:
  ├── index
  ├── att_name        — имя файла (например "карточка_123.pdf")
  ├── content_type    — MIME-тип (application/pdf)
  ├── data_size       — размер в байтах
  ├── rel_globalid    — связь с родительским объектом
  └── has_data        — есть ли бинарные данные (зависит от версии GDAL)
```

**Что НЕ индексируем в manifest (только по live-запросу):**
- Геометрию отдельных объектов (слишком тяжело)
- Значения всех строк (для слоёв с >1000 объектов)
- Бинарные данные вложений (хранятся отдельно)

---

### 2. ArcGIS Pro Project (.aprx)

**Инструмент:** `zipfile.ZipFile()` → парсинг JSON (формат CIM)

.aprx — это ZIP-архив. Внутри:
- `Index.json` — граф всех узлов (слои, карты, layouts)
- `map/map.json` — описание карты, порядок слоёв, datum transforms
- `Map/<layer>.json` — описание каждого слоя (CIMFeatureLayer)
- `layout/layout.json` — настройки компоновки
- `Metadata/*.xml` — метаданные
- `GISProject.json` — основной файл проекта (большой, >1MB)

**Извлекаем из `Map/<layer>.json`:**

```
display_name          — поле "name" в JSON (в формате \uXXXX Unicode)
                        Пример: "\u041f\u043e\u043b\u0435 \u0434\u0435\u043b\u044c\u0442\u0430 G (\u043c\u0413\u0430\u043b)"
                        → "Поле дельта G (мГал)"
dataset_name          — поле "dataset" в dataConnection
feature_dataset       — поле "featureDataset" (группировка в .gdb: Study, Licences, Grid)
description           — поле "description"
visibility            — был ли слой включён в проекте
layer_type            — "Operational" / "BasemapBackground"
display_field         — какое поле показывается по умолчанию
label_expression      — выражение для подписей ($feature.Имя)
field_aliases         — алиасы полей из fieldDescriptions[].alias
```

**Извлекаем из `Map/New_Group_Layer*.json` (группы слоёв):**

```
group_name            — например "Гравика R-42", "Магнитка R-42", "Изученность"
group_members[]       — CIMPATH ссылки на слои-члены группы
```

**Извлекаем из `map/map.json`:**

```
map_name              — название карты ("Лекын_Тальбейское")
layer_order[]         — порядок слоёв (сверху вниз)
datum_transforms[]    — какие трансформации CRS используются
                        (например GSK-2011_to_WGS_1984_1, Pulkovo_1942_To_WGS_1984_20)
```

**Извлекаем из подложек (basemaps):**

```
basemaps[]            — World Topographic Map, World Hillshade (URL сервисов)
```

### 2.1. Маппинг dataset ↔ display name (центральный элемент ingestion)

Маппинг — это таблица соответствия между техническим именем слоя в .gdb и
человекочитаемым названием из .aprx. Это **критически важный** этап, без которого
агент не может корректно интерпретировать данные.

**Проблема на примере Лекын-Тальбейского проекта:**

```
gdb dataset     Что казалось         Что на самом деле (из .aprx)
───────────     ─────────────        ───────────────────────────
fhg_gr          Гравиразведка?       Полный гориз. градиент дельта G (Э)        ← ГРАВИКА
gms_r           Магниторазведка?     Поле дельта G (мГал)                       ← ГРАВИКА (не магнитка!)
fhg_m           ???                  Полный гориз. градиент дельта T (нТл/км)   ← МАГНИТКА
mms_r           ???                  Поле дельта T (x100 нТл)                   ← МАГНИТКА
extr_otr        Экстремумы?          Отрицательные экстремумы (x100 нТл)        ← МАГНИТКА
lin             Линии?               Линеаменты по гравике                      ← ГРАВИКА
```

Без .aprx агент перепутал бы гравику с магниткой — критическая ошибка для геолога.

**Алгоритм построения маппинга:**

```
Шаг 1. Распаковать .aprx как ZIP
Шаг 2. Для каждого Map/<file>.json:
        - Прочитать поле "name" → display_name (декодировать \uXXXX)
        - Прочитать dataConnection.dataset → dataset_name
        - Прочитать dataConnection.featureDataset → feature_dataset
        - Прочитать "description" → description
        - Результат: {dataset_name → display_name, feature_dataset, description}
Шаг 3. Для каждого Map/New_Group_Layer*.json:
        - Прочитать "name" → group_name
        - Прочитать "layers" → list of CIMPATH
        - Разрезолвить CIMPATH → dataset_name через маппинг из шага 2
        - Результат: {group_name → [dataset_names]}
Шаг 4. Собрать финальный маппинг:
        mapping = {
            dataset_name: {
                "display_name": "...",
                "display_name_source": "aprx",      ← откуда взято
                "group": "...",
                "feature_dataset": "...",
                "description": "...",
                "units": "..."  ← извлечь из display_name если есть (мГал, нТл, Э)
            }
        }
Шаг 5. Для слоёв из .gdb, которых нет в .aprx:
        - display_name = dataset_name (как есть)
        - display_name_source = "gdb_only"
        - Добавить в quality.warnings: "Слой X найден в .gdb, но отсутствует в .aprx"
```

**Формат маппинга в manifest:**

```json
{
  "layer_mapping": [
    {
      "dataset_name": "gms_r",
      "display_name": "Поле дельта G (мГал)",
      "display_name_source": "aprx",
      "group": "Гравика R-42",
      "feature_dataset": null,
      "description": "gms_r",
      "units": "мГал",
      "aprx_file": "Map/gms_r.json",
      "aprx_visibility": false,
      "aprx_display_field": "ID",
      "aprx_label_expression": null
    },
    {
      "dataset_name": "Скважины_ГСК",
      "display_name": "Скважины_ГСК",
      "display_name_source": "aprx",
      "group": null,
      "feature_dataset": "Study",
      "description": "Скважины_ГСК",
      "units": null,
      "aprx_file": "________________/____________.json",
      "aprx_visibility": true,
      "aprx_display_field": "Имя",
      "aprx_label_expression": "$feature.Имя"
    }
  ],

  "unmapped_layers": [
    {
      "dataset_name": "ramka",
      "reason": "Отсутствует в .aprx — слой есть только в .gdb",
      "display_name": "ramka",
      "display_name_source": "gdb_only"
    }
  ],

  "mapping_quality": {
    "total_gdb_layers": 35,
    "mapped_from_aprx": 24,
    "unmapped": 11,
    "coverage_percent": 68.6,
    "has_groups": true,
    "groups_count": 3
  }
}
```

**Правила использования маппинга агентом:**

1. Агент **всегда** использует `display_name` при общении с пользователем,
   а `dataset_name` — при вызове инструментов (query_features и т.д.)
2. Если пользователь спрашивает "что за слой gms_r?" — агент отвечает
   "Это 'Поле дельта G (мГал)' — данные гравиметрии, группа 'Гравика R-42'"
3. Если `display_name_source == "gdb_only"` — агент предупреждает:
   "Для этого слоя нет расшифровки из проекта, название может быть неточным"
4. Единицы измерения (`units`) используются при выводе статистики:
   "Диапазон значений: от -5.2 до 12.8 мГал"

---

### 3. ArcGIS Toolbox (.atbx)

**Инструмент:** `py7zr` или `zipfile` → парсинг JSON + чтение .py файлов

.atbx — это 7-zip архив. Внутри:
- `*.tool/` — папки с описаниями инструментов
- `*.py` — Python-скрипты
- JSON-файлы с параметрами

**Извлекаем для каждого инструмента:**

```
tool_name             — название инструмента
tool_description      — описание (что делает)
tool_type             — "Script" / "Model"
parameters[]          — входные/выходные параметры:
  ├── name
  ├── display_name
  ├── data_type       — тип данных параметра
  ├── direction       — Input / Output
  ├── default_value
  └── description
script_path           — путь к .py файлу (если Script tool)
script_content        — содержимое Python-скрипта (для индексации в RAG)
```

---

## Структура manifest.json

```json
{
  "version": "1.0",
  "generated_at": "2026-02-19T12:00:00Z",
  "generator": "gis-ingestion-pipeline v0.1",

  "project": {
    "id": "lekyn-talbey",
    "name": "Лекын-Тальбейская площадь",
    "source_files": {
      "gdb": "Lekyn_Talbey.gdb",
      "aprx": "Lekyn_Talbey.aprx",
      "atbx": null
    },

    "license": {
      "id": "СЛХ025834ТП",
      "region": "Ямало-Ненецкий автономный округ",
      "minerals": "Au рудное, Cu, Mo",
      "holder": "ООО \"ГеоСервисПроект\"",
      "valid_from": "17.07.2024",
      "valid_to": "17.07.2031",
      "status": "Действует"
    },

    "map": {
      "name": "Лекын_Тальбейское",
      "primary_crs": "EPSG:7683 (ГСК-2011)",
      "datum_transforms": [
        "GSK-2011_to_WGS_1984_1",
        "Pulkovo_1942_To_WGS_1984_20"
      ],
      "extent_wgs84": {
        "min_lon": 65.2,
        "min_lat": 68.1,
        "max_lon": 66.1,
        "max_lat": 68.4
      }
    }
  },

  "groups": {
    "Гравика R-42": {
      "layers": ["gms_r", "fhg_gr", "izol_grav", "lin"],
      "description": "Гравиметрические данные по листу R-42"
    },
    "Магнитка R-42": {
      "layers": ["mms_r", "fhg_m", "izol_mag", "extr_otr", "extr_pol", "n_pole"],
      "description": "Магниторазведочные данные по листу R-42"
    },
    "Изученность": {
      "layers": ["gr_iz", "mag_iz", "opmar", "Izuch_A_sel"],
      "description": "Изученность территории"
    },
    "Опробование": {
      "layers": ["Скважины_ГСК", "Канавы_ГСК"],
      "feature_dataset": "Study",
      "description": "Данные ГСК: скважины и канавы"
    },
    "Топооснова": {
      "layers": ["river", "lake", "road", "town", "relief", "obl_p"],
      "description": "Топографическая основа"
    },
    "Лицензии": {
      "layers": ["СЛХ_025834_ТП"],
      "feature_dataset": "Licences"
    },
    "Номенклатура": {
      "layers": ["GridSheet"],
      "feature_dataset": "Grid"
    },
    "Геология 3D": {
      "layers": ["BaseA_R_42", "ChemA_R_42", "DplcL_R_42", "DrudP_R_42", "MranA_R_42", "TectL_R_42"],
      "description": "3D геологическая модель по листу R-42"
    }
  },

  "layers": [
    {
      "layer_id": "gms_r",
      "display_name": "Поле дельта G (мГал)",
      "group": "Гравика R-42",
      "geometry_type": "Point",
      "feature_count": 102216,
      "crs_epsg": 7683,
      "extent_wgs84": {"min_lon": 65.2, "min_lat": 68.1, "max_lon": 66.0, "max_lat": 68.35},
      "fields": [
        {"name": "ID", "dtype": "float64"},
        {"name": "ID_1", "dtype": "float64"},
        {"name": "ID_12", "dtype": "float64"},
        {"name": "ID_123", "dtype": "float64", "description": "Значение поля, мГал"}
      ],
      "visibility_in_project": false,
      "display_field": "ID",
      "source": {
        "from_gdb": true,
        "from_aprx": true,
        "aprx_file": "Map/gms_r.json"
      }
    },
    {
      "layer_id": "Скважины_ГСК",
      "display_name": "Скважины_ГСК",
      "group": "Опробование",
      "feature_dataset": "Study",
      "geometry_type": "Point",
      "feature_count": 105,
      "crs_epsg": 7683,
      "fields": [
        {"name": "Id", "dtype": "int"},
        {"name": "Имя", "dtype": "str", "unique_count": 105},
        {"name": "Участ", "dtype": "str"},
        {"name": "POINT_X", "dtype": "float64"},
        {"name": "POINT_Y", "dtype": "float64"}
      ],
      "label_expression": "$feature.Имя"
    },
    {
      "layer_id": "Izuch_A_sel",
      "display_name": "Izuch_A_sel",
      "group": "Изученность",
      "geometry_type": "MultiPolygon",
      "feature_count": 89,
      "crs_epsg": 7683,
      "fields": [
        {"name": "web_uk_id", "dtype": "str"},
        {"name": "vid_iz", "dtype": "str", "unique_count": "N",
         "top_values": {"Геологическая съёмка": "N", "...": "N"}},
        {"name": "tgf", "dtype": "str"},
        {"name": "method", "dtype": "str"},
        {"name": "scale", "dtype": "str"},
        {"name": "org_isp", "dtype": "str"},
        {"name": "god_nach", "dtype": "str", "description": "Год начала работ"},
        {"name": "god_end", "dtype": "str", "description": "Год окончания работ"},
        {"name": "name_otch", "dtype": "str", "description": "Название отчёта"},
        {"name": "avts", "dtype": "str", "description": "Авторы"}
      ],
      "attachments": {
        "table": "Izuch_A_sel__ATTACH",
        "count": 89,
        "link_field": "REL_GLOBALID",
        "content_types": ["application/pdf"]
      }
    }
  ],

  "attachments_summary": {
    "total": 89,
    "tables": ["Izuch_A_sel__ATTACH"],
    "content_types": {"application/pdf": 89},
    "extractable": true
  },

  "rasters": [
    {
      "name": "ГК_200_Карта_ПИ_Душин_jpg",
      "display_name": "ГК_200_Карта_ПИ_Душин_jpg",
      "description": "Геологическая карта ПИ масштаба 1:200k (Душин)",
      "source": "from_aprx"
    }
  ],

  "toolbox": null,

  "quality": {
    "layers_total": 35,
    "layers_non_empty": 32,
    "layers_with_display_name": 13,
    "layers_with_unknown_meaning": 11,
    "attachments_extractable": true,
    "crs_consistent": true,
    "primary_crs": "EPSG:7683",
    "has_3d_layers": true,
    "has_rasters": true,
    "metadata_completeness": "low"
  },

  "aliases": {
    "gms_r": ["гравика", "поле дельта g", "гравитационное поле", "мгал"],
    "mms_r": ["магнитка", "поле дельта t", "магнитное поле", "нтл"],
    "fhg_gr": ["градиент гравики", "горизонтальный градиент g"],
    "fhg_m": ["градиент магнитки", "горизонтальный градиент t"],
    "izol_grav": ["изолинии гравики"],
    "izol_mag": ["изолинии магнитки"],
    "extr_otr": ["отрицательные экстремумы", "минимумы магнитного поля"],
    "extr_pol": ["положительные экстремумы", "максимумы магнитного поля"],
    "lin": ["линеаменты"],
    "Скважины_ГСК": ["скважины", "бурение", "wells"],
    "Канавы_ГСК": ["канавы", "траншеи", "trenches"],
    "Izuch_A_sel": ["изученность", "работы", "исследования"],
    "gr_iz": ["гравиметровые съёмки", "гравиизученность"],
    "mag_iz": ["аэромагнитная изученность", "магнитоизученность"],
    "relief": ["рельеф", "горизонтали", "высоты"],
    "river": ["реки", "гидрография"],
    "lake": ["озёра"],
    "road": ["дороги"],
    "GridSheet": ["листы", "номенклатура", "разграфка"]
  }
}
```

---

## Ingestion pipeline

```
Шаг 1: Валидация входных файлов
  ├── Проверить что .gdb существует и читается fiona
  ├── Проверить что .aprx существует и распаковывается как zip
  │   └── Если .aprx отсутствует → ПРЕДУПРЕЖДЕНИЕ:
  │       "Загрузка без .aprx. Названия слоёв будут техническими,
  │        маппинг display names невозможен. Рекомендуется загрузить .aprx"
  │       → Продолжить с пометкой display_name_source="gdb_only" для всех слоёв
  └── Проверить .atbx (если есть) — распаковывается как 7z

Шаг 2: Построение маппинга из .aprx (КЛЮЧЕВОЙ ШАГ)
  ├── Распаковка .aprx как zip → временная директория
  ├── Парсинг Map/*.json:
  │   ├── Для CIMFeatureLayer / CIMRasterLayer:
  │   │   ├── display_name ← поле "name" (декодирование \uXXXX → Unicode)
  │   │   ├── dataset_name ← dataConnection.dataset
  │   │   ├── feature_dataset ← dataConnection.featureDataset
  │   │   ├── units ← извлечь из display_name регулярками: (мГал), (нТл), (Э), (м), (км)
  │   │   ├── display_field ← featureTable.displayField
  │   │   ├── label_expression ← labelClasses[0].expression
  │   │   └── visibility ← поле "visibility"
  │   └── Для CIMGroupLayer:
  │       ├── group_name ← поле "name"
  │       └── members[] ← поле "layers" (CIMPATH → dataset_name через маппинг)
  ├── Парсинг map/map.json:
  │   ├── map_name ← поле "name"
  │   ├── layer_order ← поле "layers"
  │   └── datum_transforms ← поле "datumTransforms"
  └── Результат: mapping{dataset_name → display_name, group, units, ...}

Шаг 3: Чтение .gdb с применением маппинга
  ├── fiona.listlayers() → список слоёв
  ├── Для каждого слоя:
  │   ├── Базовое: schema, feature_count, CRS, extent
  │   ├── Маппинг: подставить display_name, group, units из шага 2
  │   │   └── Если слой не найден в маппинге → display_name_source="gdb_only"
  │   ├── Для непустых слоёв (<10k объектов): статистика по полям
  │   ├── Для больших слоёв (>10k): только schema + count + extent
  │   └── Поиск таблиц вложений (*__ATTACH) → attachments_summary
  └── Результат: layers[] с display_name и статистикой

Шаг 4: Обогащение из .atbx (если есть)
  └── Распаковка 7z → парсинг JSON + чтение .py
  └── Извлечение tool_name, parameters, descriptions
  └── Сохранение script_content для RAG-индексации

Шаг 5: Нормализация и aliases
  └── Генерация aliases для каждого слоя:
      - из display_name: токены, lowercase
      - из dataset_name: lowercase, без подчёркиваний
      - транслит RU↔LAT
      - синонимы из словаря (geology/геология, faults/разломы, ...)
      - единицы → тип данных (мГал → гравика, нТл → магнитка)

Шаг 6: Quality score
  ├── Маппинг: coverage_percent (сколько слоёв из .gdb покрыты маппингом из .aprx)
  ├── Полнота: display_names, metadata, descriptions
  ├── CRS-консистентность
  ├── Наличие вложений и их извлекаемость
  └── Warnings: unmapped layers, empty layers, missing metadata

Шаг 7: Сохранение
  ├── manifest.json → projects/{project_id}/manifest.json
  ├── layer_mapping.json → отдельный файл маппинга (для отладки и аудита)
  ├── layer_profiles/*.json → детальные профили слоёв
  ├── aprx_unpacked/ → распакованные JSON из .aprx
  ├── attachments/ → извлечённые PDF (если extractable)
  └── _index.json → обновить реестр проектов
```

Шаг 7: Сохранение
  └── manifest.json → projects/{project_id}/manifest.json
  └── layer_profiles/*.json → детальные профили слоёв (отдельно)
  └── aprx_unpacked/ → распакованные JSON из .aprx
  └── attachments/ → извлечённые PDF (если extractable)
```

---

## Как агент использует manifest

| Тип вопроса | Источник ответа | Пример |
|---|---|---|
| Инвентаризация | `manifest.project` + `manifest.layers[]` | "Какие слои есть?" |
| Метаданные слоя | `manifest.layers[].fields` | "Какие поля у скважин?" |
| CRS | `manifest.project.map.primary_crs` | "В какой СК данные?" |
| Группировка | `manifest.groups` | "Какие данные по гравике?" |
| Изученность (обзор) | `manifest.layers[izuch].fields.top_values` | "Какие виды работ?" |
| Изученность (детали) | **live query** к .gdb | "Найди работы по геохимии после 2010" |
| Пространственный запрос | **live query** к .gdb | "Что в радиусе 5 км от точки?" |
| Вложения (список) | `manifest.attachments_summary` | "Есть ли PDF?" |
| Вложения (извлечение) | **live** — чтение из .gdb | "Извлеки карточку №5" |
| Семантический поиск | `manifest.aliases` + BM25/embeddings | "Найди слой про магнитку" |
| Между проектами | `manifest.project` нескольких проектов | "Сравни изученность двух площадей" |

---

## Файловая структура хранения

```
projects/
├── lekyn-talbey/
│   ├── manifest.json              ← основной файл
│   ├── layer_profiles/
│   │   ├── gms_r.json
│   │   ├── Скважины_ГСК.json
│   │   ├── Izuch_A_sel.json
│   │   └── ...
│   ├── data/
│   │   └── Lekyn_Talbey.gdb/     ← исходная .gdb (копия или симлинк)
│   ├── aprx_unpacked/
│   │   ├── Map/
│   │   │   ├── gms_r.json
│   │   │   ├── fhg_gr.json
│   │   │   └── ...
│   │   ├── map/map.json
│   │   └── Index.json
│   ├── attachments/
│   │   ├── карточка_001.pdf
│   │   ├── карточка_002.pdf
│   │   └── ...
│   └── toolbox/                   ← если был .atbx
│       ├── tools.json
│       └── scripts/
├── another-project/
│   ├── manifest.json
│   └── ...
└── _index.json                    ← реестр всех проектов
```
