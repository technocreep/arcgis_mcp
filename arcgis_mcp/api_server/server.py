"""GIS Agent — OpenAPI Server для OpenWebUI.

FastAPI-приложение, которое экспонирует GIS-инструменты как REST-эндпоинты.
OpenWebUI читает /openapi.json и превращает каждый эндпоинт в LLM-инструмент.

Подключение в Open WebUI: http://localhost:10002/openapi.json
Swagger UI: http://localhost:10002/docs

Запуск:
    uvicorn arcgis_mcp.api_server.server:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from arcgis_mcp.config import PROJECTS_DIR
from arcgis_mcp.mcp_server.project_store import ProjectStore
from arcgis_mcp.mcp_server.tools.attachments import make_tools as make_attachment_tools
from arcgis_mcp.mcp_server.tools.inventory import make_tools as make_inventory_tools
from arcgis_mcp.mcp_server.tools.izuchennost import make_tools as make_izuch_tools
from arcgis_mcp.mcp_server.tools.query import make_tools as make_query_tools
from arcgis_mcp.mcp_server.tools.viz_plot_layer import make_tools as make_plot_layer_tools
from arcgis_mcp.mcp_server.tools.viz_plot_overlay import make_tools as make_plot_overlay_tools
from arcgis_mcp.mcp_server.tools.viz_histogram import make_tools as make_plot_histogram_tools
from arcgis_mcp.mcp_server.tools.viz_interactive import make_tools as make_plot_interactive_tools

# ---------------------------------------------------------------------------
# Приложение
# ---------------------------------------------------------------------------

app = FastAPI(
    title="GIS Agent Service",
    description=(
        "Геоинформационный агент для работы с данными геологических проектов. "
        "Начни с list_projects → get_project_summary. "
        "P0-инструменты (inventory) читают из manifest — быстро. "
        "P1-инструменты (query, search, attachments) читают .gdb напрямую."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Инициализация инструментов
# ---------------------------------------------------------------------------

store = ProjectStore(str(PROJECTS_DIR))

# Мутабельный контекст текущего проекта — разделяется всеми инструментами
_state: dict = {"current_project_id": None}

_inv = make_inventory_tools(store, _state)
_qry = make_query_tools(store, _state)
_izuch = make_izuch_tools(store, _state)
_att = make_attachment_tools(store, _state)

list_projects_fn, get_project_summary_fn, list_layers_fn, describe_layer_fn = _inv
query_features_fn, summarize_layer_fn = _qry
(search_izuchennost_fn,) = _izuch
list_attachments_fn, extract_attachment_fn = _att

(plot_layer_fn,) = make_plot_layer_tools(store, _state)
(plot_overlay_fn,) = make_plot_overlay_tools(store, _state)
(plot_histogram_fn,) = make_plot_histogram_tools(store, _state)
(plot_interactive_fn,) = make_plot_interactive_tools(store, _state)


def _parse(result: str) -> Any:
    """Преобразовать JSON-строку инструмента в dict для FastAPI."""
    try:
        return json.loads(result)
    except Exception:
        return {"result": result}


# ---------------------------------------------------------------------------
# P0 — Inventory
# ---------------------------------------------------------------------------

@app.post(
    "/list_projects",
    operation_id="list_projects",
    summary="Список всех GIS-проектов",
    tags=["inventory"],
)
async def list_projects():
    """Показать список всех доступных GIS-проектов.

    Возвращает краткий список: id, название, количество слоёв.
    Используй этот инструмент **первым** при любом запросе пользователя о данных.
    """
    return _parse(list_projects_fn())


class GetProjectSummaryRequest(BaseModel):
    project_id: str = Field(..., description="Идентификатор проекта из list_projects()")


@app.post(
    "/get_project_summary",
    operation_id="get_project_summary",
    summary="Получить сводку по проекту и установить его как текущий",
    tags=["inventory"],
)
async def get_project_summary(req: GetProjectSummaryRequest):
    """Получить сводку по проекту и установить его как текущий.

    Вызывай после list_projects() чтобы выбрать проект для работы.
    После вызова все другие инструменты автоматически работают с этим проектом.
    """
    return _parse(get_project_summary_fn(req.project_id))


class ListLayersRequest(BaseModel):
    group: Optional[str] = Field(
        None, description='Фильтр по группе, например "Гравика R-42"'
    )
    include_needs_review: bool = Field(
        True, description="Включить слои без расшифровки (по умолчанию True)"
    )
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран через get_project_summary)"
    )


@app.post(
    "/list_layers",
    operation_id="list_layers",
    summary="Список слоёв проекта",
    tags=["inventory"],
)
async def list_layers(req: ListLayersRequest):
    """Показать список слоёв проекта.

    Для каждого слоя возвращает display_name, тип геометрии, количество объектов, группу.
    """
    return _parse(list_layers_fn(req.group, req.include_needs_review, req.project_id))


class DescribeLayerRequest(BaseModel):
    layer: str = Field(
        ...,
        description='Название слоя: display_name, layer_id или alias. Пример: "гравика", "скважины"',
    )
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/describe_layer",
    operation_id="describe_layer",
    summary="Подробное описание слоя: поля, статистика, CRS, extent",
    tags=["inventory"],
)
async def describe_layer(req: DescribeLayerRequest):
    """Подробное описание слоя: поля, числовая и категориальная статистика, CRS, extent, вложения.

    Принимает display_name, layer_id или alias — автоматически определяет слой.
    """
    return _parse(describe_layer_fn(req.layer, req.project_id))


# ---------------------------------------------------------------------------
# P1 — Query
# ---------------------------------------------------------------------------

class QueryFeaturesRequest(BaseModel):
    layer: str = Field(..., description="Название слоя (display_name, layer_id или alias)")
    filters: Optional[str] = Field(
        None,
        description=(
            'JSON-объект с условиями фильтрации. '
            'Пример: \'{"vid_iz": "Геологическая съёмка", "scale": "1:200000"}\'. '
            'Операторы: ">=2010", "<=100", "Слово" (вхождение).'
        ),
    )
    limit: int = Field(50, ge=1, le=500, description="Максимум объектов (по умолчанию 50, макс 500)")
    fields: Optional[str] = Field(
        None,
        description='Поля через запятую, например "Имя,Участ,POINT_X,POINT_Y". Если не указано — все поля.',
    )
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/query_features",
    operation_id="query_features",
    summary="Получить объекты слоя с фильтрацией по атрибутам",
    tags=["query"],
)
async def query_features(req: QueryFeaturesRequest):
    """Получить объекты из слоя с фильтрацией по атрибутам. Читает напрямую из .gdb.

    Используй для получения конкретных значений или когда нужно больше деталей,
    чем предоставляет describe_layer().
    """
    return _parse(query_features_fn(req.layer, req.filters, req.limit, req.fields, req.project_id))


class SummarizeLayerRequest(BaseModel):
    layer: str = Field(..., description="Название слоя (display_name, layer_id или alias)")
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/summarize_layer",
    operation_id="summarize_layer",
    summary="Вычислить актуальную статистику по полям слоя из .gdb",
    tags=["query"],
)
async def summarize_layer(req: SummarizeLayerRequest):
    """Вычислить актуальную статистику по полям слоя из .gdb.

    Используй когда describe_layer() не имеет статистики или нужны свежие данные.
    Для числовых полей: min, max, mean. Для строковых: уникальные значения и топ-20.
    """
    return _parse(summarize_layer_fn(req.layer, req.project_id))


# ---------------------------------------------------------------------------
# P1 — Izuchennost
# ---------------------------------------------------------------------------

class SearchIzuchennostRequest(BaseModel):
    query: Optional[str] = Field(
        None,
        description='Текстовый поиск по названию отчёта, авторам, организации. Пример: "аэромагнитная"',
    )
    year_from: Optional[int] = Field(
        None, description="Год начала работ не раньше (включительно)"
    )
    year_to: Optional[int] = Field(
        None, description="Год окончания работ не позже (включительно)"
    )
    work_type: Optional[str] = Field(
        None,
        description='Вид работ (частичное совпадение). Пример: "Аэромагнитная", "Геологическая съёмка"',
    )
    scale: Optional[str] = Field(
        None, description='Масштаб (частичное совпадение). Пример: "1:200000"'
    )
    limit: int = Field(30, ge=1, le=200, description="Максимум записей (по умолчанию 30, макс 200)")
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/search_izuchennost",
    operation_id="search_izuchennost",
    summary="Поиск ранее выполненных геологических работ по территории",
    tags=["izuchennost"],
)
async def search_izuchennost(req: SearchIzuchennostRequest):
    """Поиск в слоях изученности (Izuch_A_sel и подобных) по типу работ, годам, масштабу, ключевым словам.

    Используй для вопросов: "Какие работы проводились на этой территории?",
    "Есть ли аэромагнитные данные после 2000 года?" и т.д.
    """
    return _parse(
        search_izuchennost_fn(
            req.query, req.year_from, req.year_to,
            req.work_type, req.scale, req.limit, req.project_id,
        )
    )


# ---------------------------------------------------------------------------
# P1 — Attachments
# ---------------------------------------------------------------------------

class ListAttachmentsRequest(BaseModel):
    layer: Optional[str] = Field(
        None,
        description='Имя родительского слоя, например "Izuch_A_sel" или "изученность". '
                    "Если не указано — показать все вложения всех слоёв.",
    )
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/list_attachments",
    operation_id="list_attachments",
    summary="Список файлов-вложений проекта (PDF, изображения)",
    tags=["attachments"],
)
async def list_attachments(req: ListAttachmentsRequest):
    """Показать список файлов-вложений (PDF, изображения) проекта.

    Вложения хранятся в таблицах *__ATTACH в геобазе.
    """
    return _parse(list_attachments_fn(req.layer, req.project_id))


class ExtractAttachmentRequest(BaseModel):
    table: str = Field(
        ...,
        description='Имя таблицы вложений, например "Izuch_A_sel__ATTACH"',
    )
    index: int = Field(..., ge=0, description="Индекс записи (0-based, из list_attachments)")
    output_dir: str = Field(
        "./attachments_output", description="Директория для сохранения файла"
    )
    project_id: Optional[str] = Field(
        None, description="ID проекта (необязательно, если уже выбран)"
    )


@app.post(
    "/extract_attachment",
    operation_id="extract_attachment",
    summary="Извлечь файл-вложение из геобазы на диск",
    tags=["attachments"],
)
async def extract_attachment(req: ExtractAttachmentRequest):
    """Извлечь файл-вложение (PDF, изображение) из таблицы *__ATTACH на диск.

    Используй list_attachments() чтобы узнать доступные индексы.
    """
    return _parse(
        extract_attachment_fn(req.table, req.index, req.output_dir, req.project_id)
    )


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------

class PlotLayerRequest(BaseModel):
    layer_id: str = Field(..., description="ID или display_name слоя из manifest")
    project_id: Optional[str] = Field(None, description="ID проекта (необязательно, если уже выбран)")
    color_field: Optional[str] = Field(
        None,
        description="Поле для раскраски. Числовое → colorbar, категориальное → легенда. "
                    "Если None — единый цвет.",
    )
    style: str = Field(
        "auto",
        description='"auto" | "points" | "lines" | "polygons". Auto определяет по geometry_type.',
    )
    colormap: str = Field(
        "auto",
        description='Matplotlib colormap или "auto" (мГал→RdYlBu_r, нТл→RdBu_r, высоты→terrain...).',
    )
    show_license: bool = Field(True, description="Рисовать контур лицензионного участка")
    bbox_wgs84: Optional[str] = Field(
        None, description='Обрезать по bbox: "minx,miny,maxx,maxy" в WGS84. Если None — авто-extent.'
    )
    title: Optional[str] = Field(None, description="Заголовок карты (авто, если None)")
    output_format: str = Field("png", description='"png" или "svg"')


@app.post(
    "/plot_layer",
    operation_id="plot_layer",
    summary="Визуализировать один слой на статичной карте (PNG/SVG)",
    tags=["visualization"],
)
async def plot_layer(req: PlotLayerRequest):
    """Главный инструмент визуализации: один слой на статичной карте.

    Автоматически подбирает стиль рендеринга и colormap по единицам измерения из manifest.
    Числовые поля → colorbar со срезом по квантилям; категориальные → легенда tab20.
    Контур лицензии рисуется последним (show_license=True по умолчанию).
    Возвращает путь к PNG-файлу и field_stats.
    """
    return _parse(
        plot_layer_fn(
            req.layer_id, req.project_id, req.color_field, req.style,
            req.colormap, req.show_license, req.bbox_wgs84, req.title, req.output_format,
        )
    )


class PlotOverlayRequest(BaseModel):
    layers: str = Field(
        ...,
        description=(
            'JSON-массив слоёв с параметрами стиля. '
            'Пример: \'[{"layer_id":"relief","color":"brown","linewidth":0.2,"alpha":0.3},'
            '{"layer_id":"Скважины_ГСК","color":"red","markersize":15}]\'. '
            'Ключи: layer_id (обязательно), color, alpha, linewidth, linestyle, '
            'markersize, marker, edgecolor, label.'
        ),
    )
    project_id: Optional[str] = Field(None, description="ID проекта (необязательно, если уже выбран)")
    show_license: bool = Field(True, description="Рисовать контур лицензии последним")
    show_legend: bool = Field(True, description="Показывать легенду со списком слоёв")
    title: Optional[str] = Field(None, description="Заголовок карты (авто, если None)")
    output_format: str = Field("png", description='"png" или "svg"')


@app.post(
    "/plot_overlay",
    operation_id="plot_overlay",
    summary="Наложить несколько слоёв на одну карту (PNG/SVG)",
    tags=["visualization"],
)
async def plot_overlay(req: PlotOverlayRequest):
    """Сводная карта из нескольких слоёв: геология + тектоника + скважины + геофизика и т.д.

    Первый слой в массиве = подложка, последний = поверх.
    Контур лицензии рисуется после всех слоёв (zorder=10).
    Возвращает путь к PNG-файлу и список отрисованных слоёв.
    """
    return _parse(
        plot_overlay_fn(
            req.layers, req.project_id, req.show_license, req.show_legend,
            req.title, req.output_format,
        )
    )


class PlotHistogramRequest(BaseModel):
    layer_id: str = Field(..., description="ID или display_name слоя из manifest")
    field: str = Field(..., description="Имя поля для анализа")
    project_id: Optional[str] = Field(None, description="ID проекта (необязательно, если уже выбран)")
    plot_type: str = Field(
        "auto",
        description=(
            '"auto" — автоматически по dtype и числу уникальных значений; '
            '"histogram" — гистограмма с линиями mean/median; '
            '"bar" — горизонтальный барчарт; '
            '"bar_top20" — top-20 значений; '
            '"boxplot" — box-plot по группам (требует group_by).'
        ),
    )
    group_by: Optional[str] = Field(
        None, description='Поле группировки для boxplot и bar. Пример: "Участ".'
    )
    bins: int = Field(50, ge=5, le=500, description="Количество бинов для гистограммы")
    title: Optional[str] = Field(None, description="Заголовок (авто, если None)")
    output_format: str = Field("png", description='"png" или "svg"')


@app.post(
    "/plot_histogram",
    operation_id="plot_histogram",
    summary="Построить статистический график по полю слоя (PNG/SVG)",
    tags=["visualization"],
)
async def plot_histogram(req: PlotHistogramRequest):
    """Статистическая визуализация атрибутов слоя.

    Автоматически выбирает тип графика: гистограмма для числовых полей,
    bar-chart для категориальных, boxplot для группировки.
    Возвращает путь к файлу и базовую статистику поля (field_stats).
    """
    return _parse(
        plot_histogram_fn(
            req.layer_id, req.field, req.project_id, req.plot_type,
            req.group_by, req.bins, req.title, req.output_format,
        )
    )


class PlotInteractiveRequest(BaseModel):
    layers: str = Field(
        ...,
        description=(
            'JSON-массив ID слоёв. Пример: \'["Скважины_ГСК","Канавы_ГСК","river"]\'. '
            'Все слои автоматически конвертируются в WGS84.'
        ),
    )
    project_id: Optional[str] = Field(None, description="ID проекта (необязательно, если уже выбран)")
    tooltip_fields: Optional[str] = Field(
        None,
        description=(
            'JSON-словарь {layer_id: [field1, field2, ...]}. '
            'Если None — поля выбираются автоматически из manifest. '
            'Пример: \'{"Скважины_ГСК": ["Имя", "POINT_Z", "Участ"]}\''
        ),
    )
    center: Optional[str] = Field(
        None, description='Центр карты "[lat, lon]". None → авто-центр по данным.'
    )
    zoom: int = Field(10, ge=1, le=20, description="Начальный масштаб (по умолчанию 10)")
    max_features_per_layer: int = Field(
        500, ge=1, le=5000,
        description="Максимум объектов на слой. Тяжёлые слои усекаются с предупреждением."
    )
    style_overrides: Optional[str] = Field(
        None,
        description=(
            'JSON-словарь переопределения стилей {layer_id: {color, weight, ...}}. '
            'Пример: \'{"river": {"color": "#4488ff", "weight": 1}}\''
        ),
    )


@app.post(
    "/plot_interactive",
    operation_id="plot_interactive",
    summary="Создать интерактивную HTML-карту (Folium) с переключением слоёв",
    tags=["visualization"],
)
async def plot_interactive(req: PlotInteractiveRequest):
    """Интерактивная карта (Folium) — навигация, tooltip'ы, переключение слоёв.

    Оптимальна для небольших наборов данных (скважины, канавы, рудные точки).
    Для тяжёлых слоёв (>500 объектов) возвращает предупреждение и усекает данные.
    Возвращает путь к .html файлу.
    """
    return _parse(
        plot_interactive_fn(
            req.layers, req.project_id, req.tooltip_fields, req.center,
            req.zoom, req.max_features_per_layer, req.style_overrides,
        )
    )
