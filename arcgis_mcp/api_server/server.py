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
import logging
import sys
from typing import Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)

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
from arcgis_mcp.mcp_server.tools.viz_plot_relief import make_tools as make_plot_relief_tools
from arcgis_mcp.mcp_server.tools.viz_histogram import make_tools as make_plot_histogram_tools
# from arcgis_mcp.mcp_server.tools.viz_interactive import make_tools as make_plot_interactive_tools
from arcgis_mcp.mcp_server.tools.datacube import make_tools as make_datacube_tools
from arcgis_mcp.mcp_server.tools.kg_query import make_tools as make_kg_query_tools
from arcgis_mcp.mcp_server.tools.work_type_lookup import make_tools as make_lookup_tools

# ---------------------------------------------------------------------------
# Приложение
# ---------------------------------------------------------------------------

app = FastAPI(
    title="GIS Agent Service",
    description=(
        "Геоинформационный агент для работы с данными геологических проектов. "
        "Начни с list_projects чтобы получить project_id. "
        "Передавай project_id явно в каждый инструмент. "
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

# Пустой контекст — передаётся в make_tools для совместимости сигнатуры
_state: dict = {}

_inv = make_inventory_tools(store, _state)
_qry = make_query_tools(store, _state)
_izuch = make_izuch_tools(store, _state)
_att = make_attachment_tools(store, _state)

list_projects_fn, get_project_summary_fn, list_layers_fn, describe_layer_fn = _inv
query_features_fn, summarize_layer_fn = _qry
search_izuchennost_fn, get_izuchennost_records_fn = _izuch
(list_attachments_fn,) = _att

(plot_layer_fn,) = make_plot_layer_tools(store, _state)
(plot_overlay_fn,) = make_plot_overlay_tools(store, _state)
(plot_relief_fn,) = make_plot_relief_tools(store, _state)
(plot_histogram_fn,) = make_plot_histogram_tools(store, _state)
# (plot_interactive_fn,) = make_plot_interactive_tools(store, _state)

(
    datacube_overview_fn,
    datacube_block_scores_fn,
    datacube_block_detail_fn,
    datacube_report_overview_fn,
    datacube_score_overlay_fn,
) = make_datacube_tools(store, _state)

(geo_context_query_fn,) = make_kg_query_tools(_state)
(lookup_work_types_fn,) = make_lookup_tools(_state)


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
    summary="Получить сводку по проекту: слои, группы, CRS, вложения",
    tags=["inventory"],
)
async def get_project_summary(req: GetProjectSummaryRequest):
    """Получить сводку по проекту.

    Возвращает: name, map (название + extent_wgs84), layers_total, layers_non_empty,
    mapping_coverage (% слоёв с расшифровкой из .aprx), groups (словарь группа→число слоёв),
    attachments_count, crs, has_3d_layers, warnings.
    Вызывай чтобы узнать доступные группы перед list_layers(group=...).
    """
    return _parse(get_project_summary_fn(req.project_id))


class ListLayersRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    group: Optional[str] = Field(
        None, description='Фильтр по группе, например "Гравика R-42". Список групп — в get_project_summary().'
    )
    include_needs_review: bool = Field(
        True, description="Включить слои без расшифровки (по умолчанию True)"
    )
    output_format: str = Field(
        "compact",
        description='"compact" (по умолчанию) — текст сгруппированный по группам, экономит контекст. '
                    '"json" — полный JSON со всеми полями, для отладки.',
    )


@app.post(
    "/list_layers",
    operation_id="list_layers",
    summary="Список слоёв проекта",
    tags=["inventory"],
)
async def list_layers(req: ListLayersRequest):
    """Показать список слоёв проекта, сгруппированных по группам.

    Для каждого слоя: layer_id, display_name, тип геометрии, количество объектов.
    layer_id используй в визуализационных инструментах и describe_layer().
    Слои с ⚠ (needs_review) найдены только в .gdb — их display_name может быть нечитаемым.
    """
    return _parse(list_layers_fn(req.group, req.include_needs_review, req.project_id, req.output_format))


class DescribeLayerRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer: str = Field(
        ...,
        description='Название слоя: display_name, layer_id или alias. Пример: "гравика", "скважины"',
    )


@app.post(
    "/describe_layer",
    operation_id="describe_layer",
    summary="Подробное описание слоя: поля, статистика, CRS, extent",
    tags=["inventory"],
)
async def describe_layer(req: DescribeLayerRequest):
    """Подробное описание слоя из manifest.

    Возвращает: geometry_type, feature_count, crs_epsg, extent_wgs84, units,
    fields (каждое поле: name, dtype, min/max/mean для числовых, unique_count/top_values для строковых).
    Для слоёв ≥10k объектов статистика берётся из layer_profiles/ (может отсутствовать — тогда используй summarize_layer).
    Принимает display_name, layer_id или alias — сервер разрешает автоматически.
    """
    return _parse(describe_layer_fn(req.layer, req.project_id))


# ---------------------------------------------------------------------------
# P1 — Query
# ---------------------------------------------------------------------------

class QueryFeaturesRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
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


@app.post(
    "/query_features",
    operation_id="query_features",
    summary="Получить объекты слоя с фильтрацией по атрибутам",
    tags=["query"],
)
async def query_features(req: QueryFeaturesRequest):
    """Выборка объектов из слоя с фильтрацией и выбором полей. Читает напрямую из .gdb.

    Возвращает: features (массив объектов с выбранными полями), count, filters_applied.
    Фильтры: JSON {поле: значение}, поддерживает операторы >=, <=, частичное совпадение.
    Используй для получения конкретных строк, когда describe_layer() недостаточно.
    """
    return _parse(query_features_fn(req.layer, req.filters, req.limit, req.fields, req.project_id))


class SummarizeLayerRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer: str = Field(..., description="Название слоя (display_name, layer_id или alias)")


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
    project_id: str = Field(..., description="ID проекта из list_projects()")


@app.post(
    "/search_izuchennost",
    operation_id="search_izuchennost",
    summary="Сводка геологических работ по десятилетиям",
    tags=["izuchennost"],
)
async def search_izuchennost(req: SearchIzuchennostRequest):
    """Обзор изученности территории: группировка по десятилетиям.

    Возвращает по каждому десятилетию: количество работ, организации, методы, масштабы.
    Используй как первый шаг. Для конкретных записей используй get_izuchennost_records.
    """
    return _parse(search_izuchennost_fn(req.project_id))


class GetIzuchennostRecordsRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    year_from: Optional[int] = Field(None, description="Год начала работ не раньше (включительно)")
    year_to: Optional[int] = Field(None, description="Год окончания работ не позже (включительно)")
    method: Optional[str] = Field(
        None, description='Метод работ (частичное совпадение). Пример: "ГДП", "РНГ", "ТЕМ-Ц"'
    )
    organization: Optional[str] = Field(
        None, description='Название организации (частичное совпадение). Пример: "Севморгео"'
    )
    limit: int = Field(50, ge=1, le=200, description="Максимум записей (по умолчанию 50, макс 200)")


@app.post(
    "/get_izuchennost_records",
    operation_id="get_izuchennost_records",
    summary="Полные записи геологических работ с фильтрацией",
    tags=["izuchennost"],
)
async def get_izuchennost_records(req: GetIzuchennostRecordsRequest):
    """Возвращает полные записи об отдельных геологических работах.

    Фильтры: year_from/year_to, method (метод работ), organization.
    id в каждой записи используй в extract_attachment() для получения PDF-карточки.
    """
    return _parse(
        get_izuchennost_records_fn(
            req.year_from, req.year_to, req.method, req.organization, req.limit, req.project_id,
        )
    )


# ---------------------------------------------------------------------------
# P1 — Attachments
# ---------------------------------------------------------------------------

class ListAttachmentsRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer: Optional[str] = Field(
        None,
        description='Имя родительского слоя, например "Izuch_A_sel" или "изученность". '
                    "Если не указано — показать все вложения всех слоёв.",
    )


@app.post(
    "/list_attachments",
    operation_id="list_attachments",
    summary="Список файлов-вложений проекта (PDF, изображения)",
    tags=["attachments"],
)
async def list_attachments(req: ListAttachmentsRequest):
    """Показать список файлов-вложений (PDF, изображения) по проекту или слою.

    Возвращает: total_attachments, total_size_bytes, by_table (таблица→count/types/size).
    Вложения хранятся в таблицах *__ATTACH в .gdb (связь через REL_GLOBALID).
    Примечание: только список; для доступа к содержимому PDF используй geo_context_query.
    """
    return _parse(list_attachments_fn(req.layer, req.project_id))




# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------

class PlotLayerRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer_id: str = Field(..., description="ID или display_name слоя из manifest")
    color_field: Optional[str] = Field(
        None,
        description="Поле для раскраски. "
                    "Числовое → градиентная заливка + colorbar. "
                    "Категориальное (строки) → palette tab20 + легенда с подписями значений. "
                    "Для полигональных геологических слоёв используй поле с типом породы, свитой или возрастом — каждая категория получит свой цвет. "
                    "Если None — единый цвет.",
    )
    style: str = Field(
        "auto",
        description=(
            'Стиль рендеринга — выбирай по типу данных, не только по количеству точек. '
            '"markers" — дискретные точки ≤500 (скважины, пробы, точки наблюдений). '
            '"scatter" — дискретные точки >500: используй для любого числа дискретных объектов '
            '(скважины, шурфы, пробы), даже если их десятки тысяч. '
            '"density" — непрерывные физические поля (магнитика, гравиметрия, сейсмика, радиометрия): '
            'точки — замеры по профилям/сети, griddata-интерполяция → contourf + изолинии. '
            '"lines" — линейный слой. '
            '"polygons" — полигональный слой.'
        ),
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
    license_margin: float = Field(
        0.20,
        description="Отступ вокруг контура лицензии — доля от max(ширина, высота). "
                    "0.20 по умолчанию. Увеличь до 0.4–0.5 для обзора соседних территорий.",
    )


@app.post(
    "/plot_layer",
    operation_id="plot_layer",
    summary="Визуализировать один слой на статичной карте (PNG/SVG)",
    tags=["visualization"],
)
async def plot_layer(req: PlotLayerRequest):
    """Визуализация одного слоя на статичной карте.

    Возвращает: markdown (готовая ссылка), url (MinIO или null), file (локальный путь).
    Стиль указывай явно (см. параметр style). Числовые поля → colorbar; категориальные → легенда tab20.
    Для слоёв рельефа/горизонталей используй plot_relief().
    Для геофизических точечных слоёв используй style="density" + color_field.
    """
    return _parse(
        plot_layer_fn(
            req.layer_id, req.project_id, req.color_field, req.style,
            req.colormap, req.show_license, req.bbox_wgs84, req.title,
            req.output_format, req.license_margin,
        )
    )


class PlotOverlayRequest(BaseModel):
    layers: str = Field(
        ...,
        description=(
            'JSON-массив слоёв. Первый = подложка, последний = поверх. '
            'Пример: \'[{"layer_id":"mms_r","style":"density","color_field":"дельта_T","colormap":"RdBu_r"},'
            '{"layer_id":"relief"}]\'. '
            'Ключи: layer_id (обязательно), style ("scatter"|"density"|"lines"|"polygons"), '
            'color_field (поле для density-интерполяции), colormap, '
            'color, alpha, linewidth, linestyle, markersize, marker, edgecolor, label. '
            'style="density"+color_field → griddata-интерполяция+contourf для точечных геофизических слоёв. '
            'Рельефные слои (relief/горизонтали) рисуются серым с подписями высот автоматически.'
        ),
    )
    project_id: str = Field(..., description="ID проекта из list_projects()")
    show_license: bool = Field(True, description="Рисовать контур лицензии последним")
    show_legend: bool = Field(True, description="Показывать легенду со списком слоёв")
    title: Optional[str] = Field(None, description="Заголовок карты (авто, если None)")
    output_format: str = Field("png", description='"png" или "svg"')
    license_margin: float = Field(
        0.20,
        description="Отступ вокруг контура лицензии — доля от max(ширина, высота). "
                    "0.20 по умолчанию. Увеличь до 0.4–0.5 для обзора соседних территорий.",
    )


class PlotReliefRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer_id: str = Field(..., description="ID или display_name слоя горизонталей рельефа")
    show_rivers: bool = Field(True, description="Отображать слои рек поверх рельефа")
    show_license: bool = Field(True, description="Рисовать контур лицензии")
    title: Optional[str] = Field(None, description="Заголовок карты (авто, если None)")
    output_format: str = Field("png", description='"png" или "svg"')
    license_margin: float = Field(
        0.20,
        description="Отступ вокруг контура лицензии — доля от max(ширина, высота). "
                    "0.20 по умолчанию. Увеличь до 0.4–0.5 для обзора соседних территорий.",
    )


@app.post(
    "/plot_relief",
    operation_id="plot_relief",
    summary="Карта изолиний рельефа с подписями высот и реками",
    tags=["visualization"],
)
async def plot_relief(req: PlotReliefRequest):
    """Специализированная карта рельефа с адаптивными подписями высот.

    Возвращает: markdown, url, file.
    Серые изолинии с автоматическими подписями высот, реки синим, контур лицензии.
    Оптимальнее plot_layer для слоёв горизонталей (рельеф, высоты).
    """
    return _parse(
        plot_relief_fn(
            req.layer_id, req.project_id, req.show_rivers,
            req.show_license, req.title, req.output_format, req.license_margin,
        )
    )


@app.post(
    "/plot_overlay",
    operation_id="plot_overlay",
    summary="Наложить несколько слоёв на одну карту (PNG/SVG)",
    tags=["visualization"],
)
async def plot_overlay(req: PlotOverlayRequest):
    """Сводная карта из нескольких слоёв: геология + тектоника + скважины + геофизика и т.д.

    Возвращает: markdown, url, file, layers_rendered, warnings.
    Порядок рендеринга: крупные полигоны → density → линии → точки (автоматически по типу).
    Контур лицензии рисуется поверх всех слоёв.
    Рельефные слои (с "relief"/"горизонт" в имени) рисуются серым с подписями высот автоматически.
    """
    return _parse(
        plot_overlay_fn(
            req.layers, req.project_id, req.show_license, req.show_legend,
            req.title, req.output_format, req.license_margin,
        )
    )


class PlotHistogramRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    layer_id: str = Field(..., description="ID или display_name слоя из manifest")
    field: str = Field(..., description="Имя поля для анализа")
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
    """Статистическая визуализация распределения атрибутов.

    Возвращает: markdown, url, file, field_stats (min, max, mean, std, count, unique_count).
    Тип графика (histogram/bar/boxplot) выбирается автоматически по dtype и числу уникальных значений.
    """
    return _parse(
        plot_histogram_fn(
            req.layer_id, req.field, req.project_id, req.plot_type,
            req.group_by, req.bins, req.title, req.output_format,
        )
    )


# class PlotInteractiveRequest(BaseModel): ...
# @app.post("/plot_interactive", ...) — временно отключено


# ---------------------------------------------------------------------------
# Data Cube
# ---------------------------------------------------------------------------

class DatacubeOverviewRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    scenario_id: Optional[str] = Field(
        None,
        description="ID сценария в report mode (regional_fast, balanced_reference, detailed_skeptical). "
                    "Если None — выбирается лучший по PR-AUC.",
    )


@app.post(
    "/datacube_overview",
    operation_id="datacube_overview",
    summary="Обзор артефактов Data Cube: метрики модели, распределение скоров, топ-3 фичи",
    tags=["datacube"],
)
async def datacube_overview(req: DatacubeOverviewRequest):
    """Первый вызов при работе с Data Cube.

    Возвращает: pr_auc, cv_scores, score_distribution (гистограмма по децилям),
    top_features (топ-3 по важности), blocks_total, mode (classic/report).
    В report mode: укажи scenario_id или None → лучший по PR-AUC.
    Для списка всех сценариев и профилей руды → datacube_report_overview().
    Для карты → datacube_score_overlay() (требует report mode).
    """
    return _parse(datacube_overview_fn(req.project_id, req.scenario_id))


class DatacubeBlockScoresRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    min_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Минимальный порог score")
    scenario_id: Optional[str] = Field(
        None,
        description="ID сценария в report mode. Если None — лучший по PR-AUC.",
    )


@app.post(
    "/datacube_block_scores",
    operation_id="datacube_block_scores",
    summary="Статистика скоров проспективности по блокам куба",
    tags=["datacube"],
)
async def datacube_block_scores(req: DatacubeBlockScoresRequest):
    """Статистика скоров по всем блокам (или выше min_score).

    Возвращает: score_stats (min/max/mean/median), driver_group_breakdown.
    Для детального профиля блока используй datacube_block_detail(block_id=...).
    В report mode укажи scenario_id.
    """
    return _parse(datacube_block_scores_fn(req.project_id, req.min_score, req.scenario_id))


class DatacubeBlockDetailRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    block_id: str = Field(..., description="ID блока из datacube_block_scores(), например 'block_2_0'")
    scenario_id: Optional[str] = Field(
        None,
        description="ID сценария в report mode. Если None — лучший по PR-AUC.",
    )


@app.post(
    "/datacube_block_detail",
    operation_id="datacube_block_detail",
    summary="Полный профиль одного блока: координаты, score, фичи, SHAP-значения",
    tags=["datacube"],
)
async def datacube_block_detail(req: DatacubeBlockDetailRequest):
    """Полный профиль одного блока.

    Возвращает: block_id, score, rank, lon/lat, все значения фич,
    shap_values (отсортированы по |значению| — первый = главный драйвер),
    dominant_driver, dominant_driver_group.
    В report mode укажи scenario_id.
    """
    return _parse(datacube_block_detail_fn(req.block_id, req.project_id, req.scenario_id))


class DatacubeReportOverviewRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")


@app.post(
    "/datacube_report_overview",
    operation_id="datacube_report_overview",
    summary="Обзор мультисценарного отчёта Data Cube: список сценариев, PR-AUC, доступные профили",
    tags=["datacube"],
)
async def datacube_report_overview(req: DatacubeReportOverviewRequest):
    """Обзор мультисценарного отчёта Data Cube.

    Возвращает список сценариев с метриками (PR-AUC, x*), лучший сценарий,
    доступные label_profile_id (профили руды) и model_profile_id (наборы фичей),
    число готовых артефактов визуализации.

    Вызывай перед datacube_score_overlay() чтобы узнать доступные параметры.
    Требует предварительного запуска мультисценарного пайплайна через UI.
    """
    return _parse(datacube_report_overview_fn(req.project_id))


class DatacubeScoreOverlayRequest(BaseModel):
    project_id: str = Field(..., description="ID проекта из list_projects()")
    scenario_id: Optional[str] = Field(
        None,
        description="ID сценария (regional_fast, balanced_reference, detailed_skeptical). "
                    "Если None — лучший по PR-AUC.",
    )
    label_profile_id: Optional[str] = Field(
        None,
        description="ID профиля руды (например 'any_occurrence'). "
                    "Если None — первый доступный. Узнай из datacube_report_overview().",
    )
    model_profile_id: Optional[str] = Field(
        None,
        description="Набор фичей модели: datacube_only | rs_only | combined. По умолчанию combined.",
    )
    quantile: str = Field(
        "q90",
        description="Порог проспективности: q90 | q95 | q99. "
                    "q90 = топ 10% блоков по score. По умолчанию q90.",
    )
    visualization_type: str = Field(
        "mask",
        description=(
            '"mask" (mask_dynamics) — закрашенные блоки выше порога quantile, '
            'цвет по score. Лучше для показа ареала перспективности. '
            '"contour" (contour_narrowing) — контурное сужение: показывает '
            'как зоны перспективности сжимаются при повышении quantile. '
            'Используй "mask" по умолчанию.'
        ),
    )
    layers: Optional[str] = Field(
        None,
        description=(
            'JSON-массив ГИС-слоёв для наложения на карту. Два формата:\n'
            'Краткий (авто-стиль): \'["layer_id1","layer_id2"]\'.\n'
            'Расширенный (с кастомизацией): \'[{"layer_id":"l1","color":"#e63946","alpha":0.8},'
            '{"layer_id":"l2","style":"density","color_field":"Au_ppm","colormap":"viridis"}]\'.\n'
            'Ключи: layer_id (обязательно), color, alpha, linewidth, markersize, marker, '
            'style ("scatter"|"density"|"lines"|"polygons"|"auto"), color_field, colormap.\n'
            'Если None — только блоки + контур лицензии.'
        ),
    )


@app.post(
    "/datacube_score_overlay",
    operation_id="datacube_score_overlay",
    summary="Карта проспективности Data Cube с наложением ГИС-слоёв",
    tags=["datacube"],
)
async def datacube_score_overlay(req: DatacubeScoreOverlayRequest):
    """Карта проспективности Data Cube с ГИС-слоями поверх.

    Блоки окрашены по score (viridis). Возвращает markdown (готовая ссылка на изображение),
    url (MinIO), layers_rendered, blocks_rendered, score_range.
    Workflow: datacube_report_overview() → datacube_score_overlay().
    Для наложения слоёв используй параметр layers (краткий или расширенный формат с color/style).
    """
    return _parse(
        datacube_score_overlay_fn(
            req.project_id,
            req.scenario_id,
            req.label_profile_id,
            req.model_profile_id,
            req.quantile,
            req.visualization_type,
            req.layers,
        )
    )


# ---------------------------------------------------------------------------
# Knowledge Graph
# ---------------------------------------------------------------------------

class GeoContextQueryRequest(BaseModel):
    query: str = Field(
        ...,
        description=(
            "Запрос на естественном языке к семантическому графу геологических данных. "
            "Примеры: 'карточки изученности по меди в R-42', "
            "'работы 1960-1980 в Тюменской области', "
            "'какие слои связаны с аномалией Au'."
        ),
    )
    project_id: Optional[str] = Field(
        None,
        description="Фильтр по ID проекта. Если None — поиск по всем проектам.",
    )


@app.post(
    "/geo_context_query",
    operation_id="geo_context_query",
    summary="Семантический запрос к Knowledge Graph геологических данных",
    tags=["knowledge_graph"],
)
async def geo_context_query(req: GeoContextQueryRequest):
    """Семантический поиск в Knowledge Graph (история работ, связи, справочники).

    Возвращает: query, cypher (сгенерированный Cypher), count, results (массив записей).
    Используй для:
    - связей (полезное ископаемое → работы → организации)
    - истории (какие работы в районе за 1980-2000, какие организации)
    - пространственного охвата (какие карточки покрывают эту точку/слой)
    Для атрибутного поиска по изученности используй search_izuchennost (быстрее).
    """
    return _parse(geo_context_query_fn(req.query, req.project_id))


class LookupWorkTypesRequest(BaseModel):
    codes: list[str] = Field(
        ...,
        description=(
            "Список аббревиатур кодов видов геологических работ (поле 8 карточки изученности). "
            "Пример: ['ГС', 'ТЕМ-гф', 'ПР', 'АМС']"
        ),
    )


@app.post(
    "/lookup_work_types",
    operation_id="lookup_work_types",
    summary="Расшифровка кодов видов геологических работ",
    tags=["knowledge_graph"],
)
async def lookup_work_types(req: LookupWorkTypesRequest):
    """Расшифровать аббревиатуры кодов видов работ по справочнику Росгеолфонда 1995 г.

    Используй когда нужно расшифровать коды из KG-запросов или карточек изученности:
    - ГС → Геологическая съёмка, полистная
    - ТЕМ-гф → Тематические работы, геофизическая специализация
    - ГДП → Геологическое доизучение ранее заснятых площадей
    - АМС → Аэромагнитная съёмка
    Также распознаёт не-коды: ТГФ, НТС, ГКЗ (поясняет, что это не виды работ).
    """
    return _parse(lookup_work_types_fn(req.codes))
