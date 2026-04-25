# ArcGIS MCP Server

[🇬🇧 English](README_eng.md)

Сервер реализует протокол [Model Context Protocol (MCP)](https://modelcontextprotocol.io) поверх геопространственных данных из ArcGIS File Geodatabase (`.gdb`) с опциональными метаданными из ArcGIS Pro проекта (`.aprx`). LLM-агент подключается к серверу и получает набор инструментов для поиска, анализа и визуализации геологических, геофизических и картографических данных.

**Технологический стек:** Python 3.12, FastAPI, GeoPandas, Fiona, Matplotlib, Folium, Neo4j, MinIO (S3).

---

## Общая схема системы

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Open WebUI (браузер)                                │
│                                                                              │
│  Пользователь: "Покажи карту гравиметрии для проекта Лекын"                  │
│       │                                                          ▲           │
│       │  LLM решает вызвать инструмент                           │           │
│       ▼                                                          │           │
│  [ LLM-агент ] ──── читает /openapi.json ───────────────────────-┘           │
└────────────────────────────┬─────────────────────────────────────────────────┘
                             │  POST /plot_layer
                             │  {"layer": "гравиметрия"}
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    gis-mcp  :10002  (api_server/server.py)                  │
│                                                                             │
│  Инструменты (mcp_server/tools/):                                           │
│  ┌─────────────┐  ┌────────────┐  ┌───────────┐  ┌──────────┐  ┌────────┐   │
│  │  inventory  │  │   query    │  │    viz    │  │datacube  │  │  KG    │   │
│  │  (P0)       │  │  (P1)      │  │(plot_*)   │  │          │  │ query  │   │
│  └──────┬──────┘  └─────┬──────┘  └─────┬─────┘  └────┬─────┘  └───┬───-┘   │
└─────────┼───────────────┼───────────────┼─────────────┼────────────┼──────-─┘
          │               │               │             │            │
          ▼               ▼               ▼             ▼            ▼
    manifest.json     .gdb (via      .gdb + MinIO    data-cube    Neo4j KG
    (быстро, <100мс)  GeoPandas)     (PNG/HTML)      :внутр.сеть  :7687
                      (1–30 с)       (2–60 с)
                                          │
                                          ▼
                                   MinIO / S3 (gis-viz)
                                   PNG → публичный URL
                                          │
                             возвращает markdown-ссылку
                                   на изображение


─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  Поток инжеста (отдельный путь)  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

┌──────────────────────────────────────────────────────────────────────────────┐
│                   gis-loader  :10003  (ingestion/app.py)                     │
│                                                                              │
│  POST /api/projects/upload  ◄── ZIP (.gdb + .aprx)  ◄── пользователь/UI      │
│                │                                                             │
│                ▼  pipeline.py (7 шагов)                                      │
│  1. Распаковка ZIP                                                           │
│  2. Поиск .aprx / .gdb                                                       │
│  3. Парсинг .aprx  ──► display_name, группы, CRS                             │
│  4. Парсинг .gdb   ──► поля, статистика, экстент, вложения                   │
│  5. Сборка manifest.json                                                     │
│  6. Quality checks                                                           │
│  7. KG-индексация ─────────────────────────────────────────► Neo4j KG        │
│                           (pdf_parser → InvestigationCard узлы)              │
│                           (non-blocking: ошибка не ломает pipeline)          │
│                │                                                             │
│                ▼                                                             │
│  PROJECTS_DIR/{project_id}/manifest.json  ◄── shared Docker volume           │
└──────────────────────────────────────────────────────────────────────────────┘

              ▲ shared volume "projects" ▼
   gis-mcp читает те же файлы, что записал gis-loader


─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  KG-запрос (geo_context_query)  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

  Пользователь: "Какие организации вели съёмку на золото в Лекыне?"
       │
       ▼
  gis-mcp: geo_context_query(query)
       │
       ▼
  nl_to_cypher.py ──► vLLM (KG_LLM_MODEL) ──► Cypher-запрос
       │
       ▼
  Neo4j  MATCH (ic:InvestigationCard)-[:TARGETS]->(m:Mineral {name:"золото"})
         MATCH (ic)-[:CONDUCTED_BY]->(o:Organization)
         RETURN o.name, ic.year_start, ic.title  LIMIT 50
       │
       ▼
  JSON-результат ──► агент формирует ответ пользователю
```

---

## Быстрый старт

```bash
# 1. Скопировать шаблон и заполнить переменные
cp .env.template .env

# 2. Поднять стек
docker compose up -d

# 3. Открыть
# Ingestion UI: http://localhost:10003/ui/
# MCP Swagger:  http://localhost:10002/docs
# Neo4j:        http://localhost:7474
```

**Docker-сервисы:**

| Сервис | Порт | Назначение |
|--------|------|-----------|
| `gis-loader` | 10003 | Ingestion API — загрузка `.aprx`/`.gdb` ZIP, запуск pipeline, KG-индексация |
| `gis-mcp` | 10002 | OpenAPI Tool Server — инструменты для Open WebUI агента |
| `neo4j` | 7474 / 7687 | Knowledge Graph (Community Edition) |
| `data-cube` | — | ML-пайплайн проспективности (`Dockerfile.datacube`) |

**Make-команды:**

```bash
make rebuild       # Полная пересборка всего стека
make rebuild-app   # Пересобрать только gis-loader + gis-mcp
make reload-app    # Перезапустить app-контейнеры без пересборки (правки кода)
make rebuild-cube  # Пересобрать data-cube с нуля
make update-cube   # git pull + pip install в контейнере data-cube
```

---

## Архитектура

```
arcgis_mcp/
├── config.py                     # Глобальная конфигурация (env vars)
├── mcp_server/
│   ├── server.py                 # MCP точка входа (stdio / HTTP SSE)
│   ├── project_store.py          # Абстракция доступа к данным
│   └── tools/
│       ├── inventory.py          # P0: метаданные из манифеста
│       ├── query.py              # P1: прямой доступ к .gdb
│       ├── izuchennost.py        # P1: поиск по изученности
│       ├── attachments.py        # P1: вложения (документы, фото)
│       ├── viz_utils.py          # Общие утилиты визуализации
│       ├── viz_plot_layer.py     # Статичная карта одного слоя
│       ├── viz_plot_overlay.py   # Статичная карта нескольких слоёв
│       ├── viz_histogram.py      # Гистограммы и распределения
│       ├── viz_interactive.py    # Интерактивная HTML-карта (Folium)
│       ├── datacube.py           # Data Cube: ML-артефакты из MinIO
│       ├── kg_query.py           # KG: NL-запросы к Neo4j
│       └── work_type_lookup.py   # KG: справочник видов геологических работ
├── api_server/
│   └── server.py                 # OpenAPI Tool Server (FastAPI) для Open WebUI
├── rag/
│   ├── kg_client.py              # Neo4j клиент (merge_node, merge_rel, execute)
│   ├── kg_builder.py             # Построение KG из manifest + PDF-карточек
│   ├── kg_schema.py              # Схема узлов и рёбер
│   ├── nl_to_cypher.py           # NL → Cypher через LLM
│   ├── pdf_parser.py             # Парсинг PDF карточек изученности (Vision LLM / regex)
│   ├── tile_builder.py           # SpatialTile узлы для больших слоёв
│   └── pdf_spec.json             # Справочник кодов видов работ (Росгеолфонд 1995)
├── static/datacube/
│   ├── index.html                # Viewer Data Cube
│   ├── viewer.js                 # Логика всех V-блоков
│   └── description.md            # Описание V-блоков и источников данных
└── ingestion/
    ├── pipeline.py               # Оркестрация загрузки проекта (7 шагов + KG-индексация)
    ├── parser_aprx.py            # Парсинг .aprx
    ├── parser_gdb.py             # Парсинг .gdb
    ├── manifest_builder.py       # Сборка manifest.json
    └── quality.py                # Оценка качества данных
```

### Уровни инструментов (P0 / P1 / Viz)

| Уровень | Источник данных | Скорость | Назначение |
|---------|-----------------|----------|------------|
| **P0** (Inventory) | `manifest.json` | < 100 мс | Поиск и описание слоёв без чтения .gdb |
| **P1** (Query) | `.gdb` напрямую через Fiona/GeoPandas | 1–30 с | Атрибутные запросы, живая статистика |
| **Viz** | `.gdb` + манифест | 2–60 с | Генерация карт и графиков |
| **DataCube** | ML-артефакты в MinIO (`MINIO_CUBE_BUCKET`) | < 1 с | Скоры блоков, SHAP, важность фич |

---

## Транспорт и конфигурация сервера

**Основной режим (production):** `api_server/server.py` — FastAPI OpenAPI-сервер. Open WebUI читает `/openapi.json` и превращает каждый эндпоинт в LLM-инструмент. Подключение: `http://localhost:10002/openapi.json`.

**Альтернативный режим (local/stdio):** `mcp_server/server.py` — FastMCP экземпляр для прямого запуска через `run_agent.py` / pydantic-ai.

**Разделяемое состояние** хранится в `_state = {"current_project_id": None}`. При вызове `get_project_summary(project_id=X)` поле `current_project_id` устанавливается на `X`, и все последующие инструменты не требуют повторного указания проекта.

---

## Фабрика инструментов: паттерн make_tools

Каждый модуль экспортирует функцию `make_tools(store, state) -> list[Callable]`. Внутри создаются замыкания, захватывающие `store` и `state`:

```python
def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    def plot_layer(layer_id: str, ...) -> str:
        pid = state.get("current_project_id") or project_id
        gdb = store.get_gdb_path(pid)
        ...
    return [plot_layer]
```

FastMCP видит обычные функции без `store`/`state` в сигнатуре, а зависимости инжектируются через замыкание.

---

## ProjectStore — абстракция хранилища

`project_store.py` предоставляет единый интерфейс чтения данных:

| Метод | Возвращает |
|-------|-----------|
| `list_projects()` | Список `ProjectSummary` из `_index.json` |
| `get_manifest(project_id)` | Полный `manifest.json` проекта |
| `get_layer_entry(manifest, layer_id)` | Запись слоя из манифеста |
| `get_gdb_path(project_id)` | Путь к файлу `.gdb` |
| `get_layer_profile(project_id, layer_id)` | Детальный профиль слоя из `layer_profiles/` |
| `resolve_layer_name(project_id, query)` | Нечёткое сопоставление имени |

### Разрешение имён слоёв (5 уровней, приоритет по убыванию)

1. Точное совпадение по `dataset_name` (без учёта регистра)
2. Точное совпадение по `display_name`
3. Точное совпадение по любому алиасу
4. Частичное совпадение по `display_name` (все токены запроса входят в имя)
5. Частичное совпадение по алиасам

---

## Извлечение признаков при инжесте

### Источники данных

При добавлении проекта (`ingestion/pipeline.py`) данные извлекаются из двух источников:

**`.aprx` (ArcGIS Pro Project):**
- `display_name` каждого слоя (имя в легенде карты)
- Принадлежность к группам (`group`)
- `label_expression` (поле для подписей объектов)
- `display_field` (поле для всплывающих подсказок)
- Порядок слоёв, видимость

**`.gdb` (File Geodatabase):**
- Геометрический тип (`geometry_type`): Point, LineString, Polygon и Мulti-варианты
- Число объектов (`feature_count`)
- CRS (EPSG код, WKT)
- Экстент в нативной CRS и в WGS84
- Полная схема полей с типами данных
- Статистика по полям (для слоёв ≤ 10 000 объектов):
  - числовые: min, max, mean, std, nulls
  - категориальные: unique_count, top_values (20 значений)
- Наличие таблиц вложений (`*__ATTACH`)

### Что записывается в manifest.json

```json
{
  "version": "1.0",
  "project": { "id", "name", "source_files", "map": { "primary_crs", "extent_wgs84" } },
  "layers": [
    {
      "layer_id": "gms_r",
      "display_name": "Поле дельта G (мГал)",
      "display_name_source": "aprx | gdb_only | inferred",
      "group": "Гравика R-42",
      "feature_dataset": null,
      "geometry_type": "Point",
      "feature_count": 102216,
      "crs_epsg": 7683,
      "extent_wgs84": [minx, miny, maxx, maxy],
      "units": "мГал",
      "needs_review": false,
      "fields": [ { "name", "dtype", "alias", "nulls", "min", "max", "mean" } ],
      "default_color_field": "Значение",
      "aprx_label_expression": "$feature.ID",
      "attachments": { "table": "gms_r__ATTACH", "total": 0 }
    }
  ],
  "groups": { "Гравика R-42": { "layers": ["gms_r", "fhg_gr"] } },
  "aliases": { "gms_r": ["гравика", "gravity", "поле дельта g", "мгал"] },
  "quality": { "layers_total", "metadata_completeness", "warnings": [] },
  "mapping_quality": { "mapped_from_aprx", "mapped_from_dict", "needs_review" }
}
```

**Автогенерация алиасов:** токены из `display_name`, варианты `dataset_name` (с заменой `_` на пробел и без), транслитерация кириллицы, семантические ключевые слова по единицам измерения (мГал → "gravity", "гравика", "delta g").

**`needs_review: true`** — слой найден только в `.gdb`, отсутствует в `.aprx`; имя может быть нечитаемым.

---

## P0 Инструменты — инвентаризация

Все инструменты читают только `manifest.json`, без доступа к `.gdb`.

### `list_projects()`
Возвращает JSON со списком всех проектов: `id`, `name`, `layers_count`, `has_attachments`, `created_at`. Первый шаг в любой сессии агента.

---

### `get_project_summary(project_id)`
Комплексный обзор проекта. **Побочный эффект:** устанавливает `current_project_id` в разделяемом состоянии.

Возвращает:
- `layers_total`, `layers_non_empty`
- `mapping_coverage` (% слоёв с читаемыми именами)
- `groups` — словарь `{имя_группы: число_слоёв}`
- `has_attachments`, `attachments_count`
- `crs`, `has_3d_layers`, `metadata_completeness`
- `warnings` — список проблем качества

---

### `list_layers(group, include_needs_review, project_id, output_format)`

Список слоёв проекта с опциональной фильтрацией по группе.

**Режимы вывода:**
- `"compact"` (по умолчанию) — сгруппированный plain-text, одна строка на слой. Экономит контекстное окно агента (~80% по сравнению с JSON для 50–80 слоёв):
  ```
  project=lekyn  layers=24
  hint: describe_layer(layer=...) для деталей

  [Гравика R-42]
    gms_r    Поле дельта G (мГал)               [Point]
    fhg_gr   Полный гор. градиент дельта G (Э)   [Point]  ⚠

  [без группы]
    lin      Линеаменты по гравике               [MultiLineString]
  ```
  Значок `⚠` — слой `needs_review`.
- `"json"` — полная структура в JSON.

---

### `describe_layer(layer, project_id)`
Полное описание одного слоя. Аргумент `layer` — нечёткое имя (display_name, layer_id или алиас).

Возвращает:
- Идентификаторы, группу, `feature_dataset`, геотип, число объектов
- `crs_epsg`, `extent_wgs84`
- `units`, `label_expression`
- `fields[]` с диапазоном значений, частотными топами, количеством nulls
- `warning` если `needs_review`
- `attachments` если есть вложения

---

## P1 Инструменты — прямой доступ к .gdb

Читают данные через GeoPandas/Fiona. Медленнее P0, используются когда манифеста недостаточно.

### `query_features(layer, filters, limit, fields, project_id)`
Выборка объектов с фильтрацией.

**Формат фильтров** (`filters` — JSON строка):
```json
{"Значение": ">=5.0", "Тип": "скважина"}
```
Поддерживаемые операторы: `>=`, `<=`, `>`, `<`, точное числовое совпадение, подстрока в строковых полях.

Возвращает атрибуты объектов (без геометрии), `total_after_filter`, `returned`.

---

### `summarize_layer(layer, project_id)`
Свежая статистика из `.gdb`. Используется когда `describe_layer` не содержит stats (слои > 10 000 объектов).

Возвращает `fields_stats[]`:
- числовые поля: type, min, max, mean, std, nulls
- категориальные: type, unique_count, top_values (20 значений)

---

### `search_izuchennost(query, year_from, year_to, work_type, scale, limit, project_id)`
Поиск по слоям изученности (ранее выполненные работы на территории).

Автоопределение слоёв изученности: имена, содержащие "izuch", "изученн", "survey", "работ", "opmar".

Нечёткое сопоставление полей:
- вид работ: `vid_iz`, `type`, `vid`
- годы: `god_nach` / `god_end`
- масштаб: `scale`, `masshtab`
- название отчёта: `name_otch`, `name`, `otchet`

---

### `list_attachments(layer, project_id)`
Список файловых вложений проекта (документы, фото, отчёты). Поиск по таблицам `*__ATTACH`. Возвращает имя файла, content-type, размер, привязку к объекту.

---

### `extract_attachment(table, index, output_dir, project_id)`
Извлечение бинарного вложения на диск. Возвращает путь к сохранённому файлу.

---

## Предобработка данных (viz_utils.py)

Все визуализационные инструменты используют общий конвейер подготовки данных.

### `load_and_reproject(gdb_path, layer_id, target_epsg=4326)`
Загрузка слоя из `.gdb` через `geopandas.read_file()` с перепроецированием в целевую CRS (по умолчанию WGS84 / EPSG:4326). Исходная CRS у российских геологических данных — EPSG:7683 (ГСК-2011).

### `prepare_for_plot(gdf, max_features=50_000)`
Прореживание для производительности рендеринга:
- **Точки:** случайная выборка (`random_state=42` для воспроизводимости)
- **Линии и полигоны:** упрощение геометрии (`tolerance=0.001`)

Возвращает `(gdf, was_downsampled: bool)`.

### `clip_to_view(gdf, bounds)`
Пространственная фильтрация по bounding box. Оставляет только объекты, пересекающие прямоугольник.

### `clip_quantiles(series, low=0.02, high=0.98)`
Обрезка статистических выбросов для цветовой шкалы. Возвращает `(vmin, vmax)` для нормализации matplotlib.

### `field_stats(series)`
Статистика поля: для числовых — min/max/mean/median/std/nulls; для строковых — unique_count/top_values/nulls.

### `auto_colormap(field_name, units, display_name)`
Семантический выбор colormap:
- мГал (гравиметрия) → `"RdYlBu_r"`
- нТл (магнитометрия) → `"RdBu_r"`
- высота / рельеф → `"terrain"`
- градиент → `"magma"`
- по умолчанию → `"viridis"`

Извлечение единиц: регулярное выражение `\(([^)]+)\)` из `display_name`, затем сравнение в нижнем регистре.

### Граница лицензионного участка

```python
get_license_boundary(project_id, store) -> GeoDataFrame | None
```
Автоопределение: слой, имя которого содержит `"лиценз"`, `"слх"` или `"licen"`.

```python
get_license_view_bounds(lic_gdf, margin=0.10) -> tuple | None
```
Возвращает `(minx, miny, maxx, maxy)` с отступом 10% с каждой стороны. Используется как основной extent карты — данные клипируются к этой области, а не наоборот.

```python
draw_license_boundary(ax, lic_gdf)
```
Рисует границу красным пунктиром (`linestyle="--"`, `color="red"`, `zorder=10`).

### Семантические стили слоёв

```python
get_semantic_style(layer_id, display_name, feature_dataset=None) -> dict | None
```

Таблица приоритетных правил (`_SEMANTIC_STYLE_RULES`) из 22 записей. Правило: `(паттерны_имени, паттерны_feature_dataset, стиль)`. Первое совпадение выигрывает. Поиск по конкатенации `f"{layer_id} {display_name}".lower()`.

**Категории правил:**

| Категория | Паттерны | Цвет |
|-----------|----------|------|
| Реки | river, реки | `#4488FF` linewidth 0.8 |
| Озёра | lake, озёр | `#87CEEB` alpha 0.5 |
| Дороги | road, дорог | `#888888` linewidth 0.5 |
| Населённые пункты | town, насел, город | `#8B4513` marker |
| Рельеф (изолинии) | relief, горизонт, contour | `#A0785A` linewidth 0.4 |
| Рамка / сетка | rama, ramka, frame | `#AAAAAA` alpha 0.5 |
| Граница (адм.) | obl_p, border, boundary | `#666666` linestyle `--` |
| Сетка листов | gridsheet, grid | `#CCCCCC` alpha 0.3 |
| Геофиз. изолинии | izol, изол, n_pole | `#BBBBBB` linewidth 0.3 |
| Линеаменты | lin, lineament | `#00BB44` linewidth 1.0 |
| Положит. экстремумы | extr_pol, положит | `#CC0000` marker `^` |
| Отрицат. экстремумы | extr_otr, отрицат | `#0055CC` marker `v` |
| Тектоника / разломы | tect, fault, разрывн | `#1A1A1A` linewidth 1.2 |
| Рудные точки | drud, ore, руд | `#FFD700` marker `D` |
| Геохим. ореолы | вторичн, ореол | `#FFB347` alpha 0.5 |
| Геология (полигоны) | geol, basea, mrana | `#B8E8A0` edgecolor `#4A7A30` |
| Профили | профил, profile | `#FF8C00` linewidth 0.7 |
| Шурфы | шурф | `#8B4513` marker `s` |
| Скважины | скважин, well | `#FFD700` marker `o` edgecolor `#333333` |
| Канавы | канав, trench | `#8B4513` linewidth 1.0 |
| Изученность | изучен, opmar, survey | `#90EE90` alpha 0.35 |

Если совпадения нет — используется `DEFAULT_STYLES` по типу геометрии.

---

## Визуализационные инструменты

### `plot_layer` — статичная карта одного слоя

```python
plot_layer(
    layer_id: str,
    project_id: str | None = None,
    color_field: str | None = None,   # поле для раскраски
    style: str = "auto",              # "auto"|"scatter"|"lines"|"polygons"
    colormap: str = "auto",           # "auto" или имя matplotlib colormap
    show_license: bool = True,
    bbox_wgs84: str | None = None,    # "minx,miny,maxx,maxy"
    title: str | None = None,
    output_format: str = "png",       # "png" | "svg"
) -> str
```

**Логика выбора поля для раскраски:**
1. Явный параметр `color_field` (с нечётким поиском по колонкам)
2. `default_color_field` из записи манифеста
3. Для геофизических слоёв (`units` заполнено): первое числовое поле, не входящее в `_SKIP_FIELDS`
4. Иначе — единый семантический цвет без colorbar

**Рендеринг по типу данных:**
- **Числовое поле:** scatter / colorized lines / filled polygons с colorbar, обрезка 2%–98% квантилей
- **Категориальное поле:** палитра tab20, легенда (до 20 значений)
- **Без поля:** семантический цвет из `get_semantic_style()`

**Extent карты:** если `show_license=True`, границы вида устанавливаются из `get_license_view_bounds()`.

**Прореживание:** при > 50 000 объектов — случайная выборка с предупреждением в ответе.

Возвращает JSON: `file`, `url`, `markdown`, `layer`, `display_name`, `feature_count`, `geometry_type`, `color_field`, `colormap`, `style`, `field_stats`, `warning`.

---

### `plot_overlay` — совмещённая карта нескольких слоёв

```python
plot_overlay(
    layers: str,            # JSON-массив layer spec объектов
    project_id: str | None = None,
    show_license: bool = True,
    show_legend: bool = True,
    title: str | None = None,
    output_format: str = "png",
) -> str
```

**Формат layer spec:**
```json
[
  {"layer_id": "relief",         "alpha": 0.3, "linewidth": 0.2},
  {"layer_id": "river",          "label": "Реки"},
  {"layer_id": "Скважины_ГСК",  "color": "red", "marker": "o", "markersize": 12, "label": "Скважины"}
]
```

**Приоритет стиля:** семантический стиль → `DEFAULT_STYLES` по геотипу → переопределения из spec.

**Порядок рендеринга:**
1. Загрузить границу лицензии, вычислить `view_bounds`
2. Для каждого слоя: загрузить → перепроецировать → клипировать к `view_bounds` → прорисовать
3. Нарисовать границу лицензии поверх (`zorder=10`)
4. Добавить легенду

Возвращает JSON: `file`, `url`, `markdown`, `layers_rendered`, `layers_requested`.

---

### `plot_histogram` — статистическое распределение

```python
plot_histogram(
    layer_id: str,
    field: str,
    project_id: str | None = None,
    plot_type: str = "auto",     # "auto"|"histogram"|"bar"|"bar_top20"|"boxplot"
    group_by: str | None = None, # поле группировки для boxplot
    bins: int = 50,
    title: str | None = None,
    output_format: str = "png",
) -> str
```

**Автовыбор типа графика:**
- числовое поле, < 15 уникальных значений → `"bar"`
- числовое поле, ≥ 15 → `"histogram"`
- строковое поле, ≤ 30 уникальных → `"bar"`
- строковое поле, > 30 → `"bar_top20"`

**Типы графиков:**
- `"histogram"` — гистограмма с линиями среднего и медианы
- `"bar"` / `"bar_top20"` — горизонтальная барная диаграмма по топ-значениям
- `"boxplot"` — ящики с усами, сгруппированные по `group_by`

---

### `plot_interactive` — интерактивная HTML-карта (Folium)

```python
plot_interactive(
    layers: str,                       # JSON-массив layer ID
    project_id: str | None = None,
    tooltip_fields: str | None = None, # JSON {layer_id: [fields]}
    center: str | None = None,         # "[lat, lon]"
    zoom: int = 10,
    max_features_per_layer: int = 500,
    style_overrides: str | None = None # JSON {layer_id: {color, weight, ...}}
) -> str
```

**Особенности:**
- `LayerControl` — переключение видимости слоёв
- Всплывающие подсказки: поля выбираются автоматически через `auto_tooltip_fields()` (приоритет: display_field из `.aprx` → поля типа name → первые нечисловые поля)
- Центр и zoom карты из границ лицензии (`fit_bounds`)
- Усечение слоёв: если объектов > `max_features_per_layer`, используются первые N с предупреждением

Возвращает JSON: `file`, `url`, `link` (markdown-ссылка), `layers_rendered`, `map_center`, `zoom`, `warnings[]`.

---

## Хранилище артефактов — MinIO

Все визуализационные инструменты сохраняют файлы локально и загружают в MinIO (S3-совместимое объектное хранилище).

### Конфигурация

```python
MINIO_ENDPOINT    = "ip:9000"       # внутренний адрес (docker-сеть)
MINIO_PUBLIC_HOST = "localhost:9000"   # публичный адрес для URL
MINIO_ACCESS_KEY  = "minio"
MINIO_SECRET_KEY  = "password"
MINIO_BUCKET      = "gis-viz"         # бакет для визуализаций (PNG, HTML)
MINIO_CUBE_BUCKET = "gisportal"        # бакет для ML-артефактов Data Cube
```

Все параметры переопределяются через переменные окружения.

### Конвейер загрузки артефакта

```
plot_layer() / plot_overlay() / ...
    │
    ├─ save_figure(fig, pid, name, fmt)
    │    └── projects/{project_id}/viz/{name}.{fmt}  (локальный файл)
    │
    └─ upload_to_minio(local_path, project_id)
         ├── _get_minio()  — lazy singleton клиент
         ├── _ensure_bucket()  — создать бакет + public policy если не существует
         ├── put_object("gis-viz/{project_id}/{filename}", ...)
         └── return "http://{MINIO_PUBLIC_HOST}/gis-viz/{project_id}/{filename}"
```

**Политика бакета:** при создании устанавливается public read-only (анонимный GET).

**Деградация при недоступности:** если MinIO недоступен, инструмент возвращает `url: null` и `markdown: null`, указывает локальный путь в `file`. Агент уведомляет пользователя о необходимости скачать файл локально.

### Что возвращают инструменты

```json
{
  "file":     "projects/lekyn/viz/gms_r_1708772400.png",
  "url":      "http://localhost:9000/gis-viz/lekyn/gms_r_1708772400.png",
  "markdown": "![Поле дельта G (мГал)](http://localhost:9000/gis-viz/lekyn/gms_r_1708772400.png)"
}
```

Поле `markdown` позволяет агенту встроить изображение прямо в ответ (поддерживают Open WebUI, Claude Desktop и другие MCP-клиенты с рендерингом Markdown).

---

## GIS Data Hub — веб-портал (Ingestion API)

`ingestion/app.py` — FastAPI-сервис (`gis-loader`), доступный на `http://localhost:10003`.

- **Веб-портал:** `/ui/` — Vue 3 SPA (`static/index.html` + `app.js`). Управление проектами: загрузка, просмотр манифеста, запуск Data Cube, удаление.
- **Авторизация:** Basic Auth + session cookie (`gis_session`, TTL 8 ч). Все write-эндпоинты и файлы артефактов защищены.
- **Загрузка проектов:** `POST /api/upload` — принимает `.zip` с GDB и опциональный `.aprx`, запускает ingestion pipeline синхронно.
- **Прокси Data Cube:** `POST /api/datacube/jobs`, `GET /api/datacube/jobs/{id}` — проксируют запросы к сервису `data-cube`.
- **Артефакты:** `GET /api/projects/{id}/datacube/files/{path}` — раздаёт файлы результатов пайплайна с Basic Auth / session cookie / `?_auth=` query-параметром. HTML-файлы получают инжекцию `dashboard-override.css` и `lightbox.js`.
- **Cache-Control:** все `/ui/*` маршруты отдаются с `no-cache, must-revalidate` — достаточно обычного F5 для обновлений.

---

## REST API (Open WebUI)

`api_server/server.py` оборачивает все MCP-инструменты в FastAPI эндпоинты для интеграции с Open WebUI или прямого вызова через HTTP.

- **Swagger UI:** `http://localhost:10002/docs`
- **OpenAPI JSON:** `http://localhost:10002/openapi.json`

Все эндпоинты принимают те же параметры что и MCP инструменты (POST с JSON телом), возвращают те же JSON ответы. Включает эндпоинты Data Cube (`/datacube_overview`, `/datacube_block_scores`, `/datacube_block_detail`).

---

## Data Cube — ML-артефакты проспективности

Data Cube — результат запуска ML-пайплайна (`run_pipeline`) сервиса `data-cube`. Артефакты загружаются в MinIO (`MINIO_CUBE_BUCKET/{project_id}/`) и доступны через три инструмента.

### Структура артефактов

```
{project_id}/
├── blocks.csv                                    # сетка блоков: координаты, размер
├── scores.csv                                    # score проспективности (0–1) для каждого блока
├── features.csv                                  # значения фич по блокам
├── labels.csv                                    # метки: label_y, dist_nearest_ore_m, weight_w
├── eval_report.json                              # pr_auc, capture_efficiency (x*, x_star, кривая)
├── model_meta.json                               # model_type, feature_names, cv
├── run_meta.json                                 # cube_spec, label_spec, train_spec, layer_mapping
└── interpretability/
    ├── global_importance_features.csv            # feature, importance, group
    ├── global_importance_groups.csv              # group, mean, std
    ├── dominant_driver_group.csv                 # block_id, dominant_driver_group
    ├── ale_1d.csv                                # feature, bin_center, ale
    ├── shap_values.csv                           # WIDE: block_id + колонки по фичам
    ├── shap_geo_unit_summary.csv                 # geo_unit, feature, mean_shap
    └── interpret_global.json                     # сводка importance + grouped
```

### Инструменты

#### `datacube_overview(project_id?)`
Первый вызов. Читает `eval_report.json`, `model_meta.json`, `scores.csv`, `global_importance_features.csv`, `dominant_driver_group.csv`.

Возвращает:
- `artifacts_present` — список найденных файлов
- метрики модели: `model_type`, `pr_auc`, `cv_mean_pr_auc`
- `capture_efficiency`: `x_star`, `score_threshold_at_x_star`
- `score_distribution`: n_blocks, min/max/mean, high_confidence_count
- `top3_features` — топ-3 фичи по важности
- `dominant_driver_groups` — словарь `{группа: число_блоков}`
- `hint` — подсказка следующего вызова

---

#### `datacube_block_scores(project_id?, top_n=20, min_score?)`
Читает `scores.csv`, `blocks.csv`, `dominant_driver_group.csv`.

Возвращает отсортированный список блоков: `rank`, `block_id`, `score`, `lon`, `lat`, `dominant_driver_group`. `top_n` clamp [1, 200].

---

#### `datacube_block_detail(block_id, project_id?)`
Читает `scores.csv`, `blocks.csv`, `features.csv`, `shap_values.csv`, `dominant_driver_group.csv`.

Возвращает полный профиль блока:
- `location`: lon, lat, x_m, y_m, row, col, cell_size_m
- `score` + `rank_in_dataset`
- `features`: все значения фич
- `shap_values`: список `{feature, shap}` отсортированный по `|shap|`
- `dominant_driver`, `dominant_driver_group`

---

### Docker-сервис data-cube

Сервис `data-cube` в `docker-compose.yml` запускает FastAPI-сервер ML-пайплайна (`Data_cube/api/server.py`). Принимает `POST /jobs` от `gis-loader` и запускает полный experiment pipeline.

```yaml
data-cube:
  build:
    context: .
    dockerfile: Dockerfile.datacube
    args:
      GITHUB_TOKEN: ${GITHUB_TOKEN}
      CACHE_BUST: ${CACHE_BUST:-1}   # инвалидация git clone слоя
  env_file: .env
  environment:
    - PROJECTS_DIR=/app/projects
```

**Инвалидация кэша Docker** (обновление кода `Data_cube` из репозитория без `--no-cache`):
```bash
CACHE_BUST=$(date +%s) docker compose up --build data-cube -d
```

**Горячая замена server.py без пересборки образа** (через Makefile):
```bash
make reload-cube
# docker cp Data_cube/api/server.py data-cube-server:/app/data_cube/api/server.py
# docker restart data-cube-server
```

**Отслеживание прогресса по стадиям** реализовано в `Data_cube/api/server.py` через обёртки `_tracked(fn, stage_name)` над каждой функцией пайплайна. При вызове обёртки вызывается `_set_stage(job_id, stage_name)`, что обновляет поле `stage` в `_jobs`. Клиент читает его через polling `GET /jobs/{job_id}`.

Порядок стадий: `grid` → `features` → `qa` → `labels` → `training` → `evaluation` → `visualization` → `upload`.

---

## Knowledge Graph (Neo4j)

При инжесте проекта `pipeline.py` автоматически строит граф знаний в Neo4j (шаг 6/7). KG является неблокирующим: если Neo4j недоступен, pipeline завершается успешно, KG-инструменты деградируют до пустых ответов.

### Схема графа

**Узлы:** `Project`, `Group`, `Layer`, `Field`, `Attachment`, `InvestigationCard`, `Mineral`, `Organization`, `WorkMethod`, `SpatialTile`, `DatacubeBlock`

**Рёбра:**
```
(Project)-[:HAS_LAYER]          ->(Layer)
(Project)-[:HAS_GROUP]          ->(Group)
(Project)-[:HAS_BLOCK]          ->(DatacubeBlock)
(Layer)-[:HAS_FIELD]            ->(Field)
(Layer)-[:HAS_ATTACHMENT]       ->(Attachment)
(Attachment)-[:IS_CARD]         ->(InvestigationCard)
(InvestigationCard)-[:TARGETS]          ->(Mineral)
(InvestigationCard)-[:CONDUCTED_BY]     ->(Organization)
(InvestigationCard)-[:USES_METHOD]      ->(WorkMethod)
(InvestigationCard)-[:SPATIALLY_COVERS]->(Layer)
```

### PDF-парсер карточек изученности

`rag/pdf_parser.py` извлекает структурированные данные из PDF-вложений (карточки Росгеолфонд, поля 1–28). Два режима:

- **Vision LLM** (если `KG_LLM_MODEL` задан): fitz рендерит страницы в PNG → base64 → vLLM (Pixtral/Mistral).
- **Regex fallback** (`KG_LLM_MODEL` пуст): fitz извлекает текст → regex по полям карточки.

### NL → Cypher

`rag/nl_to_cypher.py` конвертирует вопросы на естественном языке в Cypher-запросы через LLM. Системный промпт содержит полную схему, канонические паттерны и 12 правил генерации (включая автоматическое `LIMIT 50` без фильтра).

### KG-инструменты

#### `geo_context_query(query, project_id?)`
Выполняет NL-запрос к Knowledge Graph. Конвертирует вопрос → Cypher → выполняет в Neo4j → возвращает результаты. Полезен для поиска связей между объектами (минералы, организации, методы работ, карточки изученности), которые недоступны через manifest или .gdb напрямую.

---

#### `lookup_work_types(codes)`
Справочник кодов видов геологических работ (поле 8 карточки изученности, классификатор Росгеолфонд 1995). Принимает список строковых кодов, возвращает расшифровку.

---

## Сводная таблица инструментов

| Инструмент | Уровень | Источник | Назначение |
|-----------|---------|----------|------------|
| `list_projects` | P0 | manifest | Список всех проектов |
| `get_project_summary` | P0 | manifest | Обзор проекта, установка контекста |
| `list_layers` | P0 | manifest | Перечень слоёв (compact или JSON) |
| `describe_layer` | P0 | manifest | Детали слоя: поля, статистика, экстент |
| `query_features` | P1 | .gdb | Выборка объектов с фильтрами |
| `summarize_layer` | P1 | .gdb | Свежая статистика полей |
| `search_izuchennost` | P1 | .gdb | Поиск по слоям изученности |
| `list_attachments` | P1 | .gdb | Список вложений (документы, фото) |
| `extract_attachment` | P1 | .gdb | Извлечение вложения на диск |
| `plot_layer` | Viz | .gdb + manifest | Статичная карта одного слоя (PNG/SVG) |
| `plot_overlay` | Viz | .gdb + manifest | Совмещённая карта нескольких слоёв |
| `plot_histogram` | Viz | .gdb + manifest | Гистограмма / барная диаграмма |
| `plot_interactive` | Viz | .gdb + manifest | Интерактивная HTML-карта (Folium) |
| `datacube_overview` | DataCube | MinIO (MINIO_CUBE_BUCKET) | Метрики модели, топ-фичи, распределение скоров |
| `datacube_block_scores` | DataCube | MinIO | Ранжированный список блоков по score |
| `datacube_block_detail` | DataCube | MinIO | Полный профиль блока: фичи, SHAP, драйвер |
| `geo_context_query` | KG | Neo4j | NL-запрос к Knowledge Graph (минералы, орг., методы) |
| `lookup_work_types` | KG | Neo4j | Расшифровка кодов видов геологических работ |
