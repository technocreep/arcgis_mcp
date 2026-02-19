# GIS Agent Service — инструкция по реализации

Прилагается: `MANIFEST_SPEC.md` — спецификация формата manifest.

---

## Что строим

Сервис из двух частей:

1. **Ingestion API** — веб-интерфейс для загрузки GIS-проектов (.gdb + .aprx + .atbx). Принимает файлы, парсит, формирует manifest, сохраняет. Закрыт аутентификацией (логин/пароль).

2. **MCP Server** — stdio-сервер для LLM-агента. Читает manifest'ы и данные загруженных проектов, отвечает на запросы агента через tool-calling.

Оба работают с общим хранилищем `projects/`.

---

## Стек

- Python 3.11+
- FastMCP — MCP-сервер
- FastAPI — Ingestion API
- GeoPandas + Fiona + PyProj + Shapely — работа с геоданными
- Файловая система — хранение проектов (без БД на первом этапе)

---

## Структура проекта

```
ARCGIS_MCP/
├── ingestion/
│   ├── app.py              — FastAPI приложение (загрузка, аутентификация)
│   ├── parser_gdb.py       — парсер .gdb (fiona/geopandas)
│   ├── parser_aprx.py      — парсер .aprx (zip → JSON, маппинг)
│   ├── parser_atbx.py      — парсер .atbx (7z → JSON + .py)
│   ├── mapping.py          — построение маппинга dataset↔display_name
│   ├── manifest_builder.py — сборка manifest.json из результатов парсеров
│   └── quality.py          — расчёт quality score
├── mcp_server/
│   ├── server.py           — FastMCP сервер с tools
│   ├── project_store.py    — чтение manifest'ов и данных из projects/
│   └── tools/
│       ├── inventory.py    — list_projects, get_project_summary, list_layers, describe_layer
│       ├── query.py        — query_features, summarize_layer
│       ├── izuchennost.py  — search_izuchennost
│       └── attachments.py  — list_attachments, extract_attachment
├── projects/               — хранилище (генерируется автоматически)
│   └── {project_id}/
│       ├── manifest.json
│       ├── layer_mapping.json
│       ├── layer_profiles/
│       ├── data/           — .gdb
│       ├── aprx_unpacked/
│       └── attachments/
├── config.py               — пути, креды, настройки
└── README.md
```

---

## Порядок реализации

### Фаза 1: Парсеры (без API, без MCP — просто функции)

**1.1. `parser_aprx.py`** — начинаем с него, потому что маппинг первичен.

Вход: путь к .aprx
Выход: `AprxData` (dataclass)

```python
@dataclass
class LayerMapping:
    dataset_name: str           # имя в .gdb
    display_name: str           # человекочитаемое из .aprx
    group: str | None           # группа слоёв
    feature_dataset: str | None
    units: str | None           # извлечь регулярками из display_name: (мГал), (нТл/км), (Э)
    description: str | None
    visibility: bool
    display_field: str | None
    label_expression: str | None
    aprx_file: str              # путь к JSON внутри .aprx

@dataclass
class AprxData:
    map_name: str
    layer_mappings: list[LayerMapping]
    groups: dict[str, list[str]]         # group_name → [dataset_names]
    datum_transforms: list[str]
    basemaps: list[str]
```

Логика:
- `zipfile.ZipFile(aprx_path)` → распаковка
- Для каждого `Map/*.json`: прочитать JSON, декодировать Unicode в поле `"name"`, извлечь `dataConnection.dataset`, `dataConnection.featureDataset`
- Для `CIMGroupLayer`: собрать группы, разрезолвить CIMPATH → dataset_name
- Для `map/map.json`: извлечь map_name, datum_transforms
- Единицы: `re.search(r'\(([^)]*(?:мГал|нТл|Э|м|км)[^)]*)\)', display_name)`

**1.2. `parser_gdb.py`**

Вход: путь к .gdb
Выход: `GdbData` (dataclass)

```python
@dataclass
class FieldProfile:
    name: str
    dtype: str
    nulls: int
    # числовые:
    min: float | None
    max: float | None
    mean: float | None
    # категориальные:
    unique_count: int | None
    top_values: dict[str, int] | None   # значение → частота, до 20 штук

@dataclass
class LayerProfile:
    layer_id: str
    geometry_type: str | None
    feature_count: int
    crs_epsg: int | None
    crs_wkt: str | None
    extent_native: dict | None          # {minx, miny, maxx, maxy}
    extent_wgs84: dict | None           # {min_lon, min_lat, max_lon, max_lat}
    fields: list[FieldProfile]
    is_attachment_table: bool            # *__ATTACH

@dataclass
class GdbData:
    layers: list[LayerProfile]
    attachment_tables: list[str]
```

Логика:
- `fiona.listlayers()` → перебор
- Для каждого слоя: `fiona.open()` → schema, len, CRS
- Для непустых слоёв <10k объектов: `gpd.read_file()` → статистика полей
- Для слоёв >10k: только schema + count + extent (без полной загрузки)
- Определить attachment-таблицы по паттерну `*__ATTACH`

**1.3. `parser_atbx.py`** (опциональный, реализовать последним)

Вход: путь к .atbx
Выход: список описаний инструментов

**1.4. `mapping.py`** — объединение результатов парсеров

```python
def build_mapping(aprx_data: AprxData, gdb_data: GdbData) -> LayerMappingResult:
    """Сопоставить слои .gdb с display names из .aprx."""
```

- Для каждого layer из gdb_data: найти соответствие в aprx_data.layer_mappings по dataset_name
- Unmapped слои → `display_name_source = "gdb_only"`, добавить в warnings
- Посчитать coverage_percent

**1.5. `manifest_builder.py`** — сборка финального manifest.json

```python
def build_manifest(
    project_id: str,
    gdb_data: GdbData,
    aprx_data: AprxData,
    mapping: LayerMappingResult,
    atbx_data: AtbxData | None = None,
) -> dict:
    """Собрать manifest.json по спецификации MANIFEST_SPEC.md."""
```

**Тест фазы 1:** запустить на Lekyn_Talbey.gdb + .aprx, получить manifest.json,
проверить что display names корректны (gms_r → "Поле дельта G (мГал)").

---

### Фаза 2: MCP Server

**2.1. `project_store.py`** — абстракция доступа к проектам

```python
class ProjectStore:
    def __init__(self, projects_dir: str): ...
    def list_projects(self) -> list[ProjectSummary]: ...
    def get_manifest(self, project_id: str) -> dict: ...
    def get_layer_profile(self, project_id: str, layer_id: str) -> dict: ...
    def get_gdb_path(self, project_id: str) -> str: ...
    def resolve_layer_name(self, project_id: str, user_query: str) -> str | None:
        """Найти layer_id по display_name, dataset_name или alias."""
```

**2.2. MCP Tools** — 8 инструментов:

| Tool | Источник данных | Приоритет |
|------|----------------|-----------|
| `list_projects` | `_index.json` | P0 |
| `get_project_summary` | `manifest.json` | P0 |
| `list_layers` | `manifest.json` | P0 |
| `describe_layer` | `manifest.json` + `layer_profiles/` | P0 |
| `query_features` | live .gdb | P1 |
| `summarize_layer` | live .gdb (или кэш в layer_profile) | P1 |
| `search_izuchennost` | live .gdb | P1 |
| `list_attachments` / `extract_attachment` | manifest + live .gdb | P1 |

P0 — работают только по manifest, реализовать первыми.
P1 — требуют чтения .gdb, реализовать вторыми.

**2.3. Контекст проекта**

MCP server хранит `current_project_id`. Агент вызывает `list_projects` → `get_project_summary(project_id)`, после чего все последующие вызовы работают в контексте этого проекта. Если агент вызывает tool без project_id и текущий проект не выбран — вернуть ошибку с подсказкой.

**Тест фазы 2:** подключить к модели на простому агенту на Openrouter (api key в .env), спросить "Какие слои в проекте Лекын-Тальбей?",
получить ответ с display names из manifest.

---

### Фаза 3: Ingestion API

**3.1. `app.py`** — FastAPI

Эндпоинты:
- `POST /auth/login` → JWT-токен
- `POST /projects/upload` — multipart: .gdb (zip), .aprx, .atbx (опционально)
- `GET /projects` — список проектов
- `GET /projects/{id}` — manifest
- `DELETE /projects/{id}`

При загрузке:
1. Сохранить файлы во временную директорию
2. Распаковать .gdb (если пришла как zip)
3. Запустить ingestion pipeline (парсеры → маппинг → manifest)
4. Переместить в `projects/{project_id}/`
5. Обновить `_index.json`

**3.2. Аутентификация** — простой BasicAuth или JWT. На первом этапе — один пользователь
из конфига (логин/пароль в `config.py` или env-переменных).

**Тест фазы 3:** загрузить через curl/UI проект, убедиться что manifest создаётся,
затем проверить через агента на базе модели в OpenRouter что новый проект доступен.

---

## Критические правила

1. **Агент всегда показывает display_name пользователю**, dataset_name — только в API-вызовах.
2. **Маппинг строится ДО чтения .gdb** — сначала .aprx, потом .gdb с подстановкой.
3. **manifest отвечает на 80% вопросов** — live query к .gdb только когда нужны конкретные значения или пространственные операции.
4. **Unmapped слои** (есть в .gdb, нет в .aprx) — помечаются, агент предупреждает пользователя.
5. **Большие слои (>10k объектов)** — в manifest только schema + count + extent, без полной статистики.
6. **Единицы измерения** из display_name — извлекаются и хранятся отдельно, используются при выводе статистики.
