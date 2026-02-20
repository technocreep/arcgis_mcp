"""Общие утилиты для визуализационных инструментов.

Используется всеми viz_*.py модулями.
"""

from __future__ import annotations

import matplotlib
matplotlib.use("Agg")  # headless — до любого импорта pyplot
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from pathlib import Path


# ---------------------------------------------------------------------------
# Загрузка и репроекция
# ---------------------------------------------------------------------------

def load_and_reproject(gdb_path: str, layer_id: str, target_epsg: int = 4326) -> gpd.GeoDataFrame:
    """Загрузить слой из .gdb и привести к целевой CRS."""
    gdf = gpd.read_file(gdb_path, layer=layer_id)
    if gdf.crs is None:
        return gdf
    if gdf.crs.to_epsg() != target_epsg:
        gdf = gdf.to_crs(epsg=target_epsg)
    return gdf


def prepare_for_plot(gdf: gpd.GeoDataFrame, max_features: int = 50_000) -> tuple[gpd.GeoDataFrame, bool]:
    """Downsample (точки) или simplify (линии/полигоны) при превышении лимита.

    Returns:
        (gdf, downsampled) — downsampled=True только для точечных слоёв.
    """
    if len(gdf) <= max_features:
        return gdf, False

    gt = gdf.geometry.geom_type.mode().iloc[0].lower() if len(gdf) > 0 else "point"
    if "point" in gt:
        return gdf.sample(n=max_features, random_state=42), True
    else:
        gdf = gdf.copy()
        gdf.geometry = gdf.geometry.simplify(tolerance=0.001)
        return gdf, False


# ---------------------------------------------------------------------------
# Контур лицензии
# ---------------------------------------------------------------------------

def get_license_boundary(project_id: str, store) -> gpd.GeoDataFrame | None:
    """Найти и вернуть контур лицензионного участка в WGS84.

    Ищет слой с feature_dataset='Licences' или содержащий 'слх'/'лиценз' в имени.
    Возвращает GeoDataFrame или None если слой не найден.
    """
    try:
        manifest = store.get_manifest(project_id)
        gdb_path = store.get_gdb_path(project_id)
    except Exception:
        return None

    license_layer_id = None
    for layer in manifest.get("layers", []):
        lid = layer["layer_id"].lower()
        dn = layer.get("display_name", "").lower()
        fd = (layer.get("feature_dataset") or "").lower()
        if (
            "лиценз" in lid or "лиценз" in dn
            or "слх" in lid or "слх" in dn
            or "licen" in fd or "licen" in lid
        ):
            license_layer_id = layer["layer_id"]
            break

    if license_layer_id is None:
        return None

    try:
        gdf = gpd.read_file(gdb_path, layer=license_layer_id)
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        return gdf
    except Exception:
        return None


def draw_license_boundary(ax: plt.Axes, lic_gdf: gpd.GeoDataFrame | None) -> None:
    """Нарисовать контур лицензии на осях (zorder=10)."""
    if lic_gdf is None or lic_gdf.empty:
        return
    lic_gdf.boundary.plot(
        ax=ax, color="red", linewidth=1.8, linestyle="--",
        label="Контур лицензии", zorder=10,
    )


def get_license_view_bounds(
    lic_gdf: gpd.GeoDataFrame | None,
    margin: float = 0.10,
) -> tuple[float, float, float, float] | None:
    """Вернуть (minx, miny, maxx, maxy) по контуру лицензии + margin%. None если нет контура."""
    if lic_gdf is None or lic_gdf.empty:
        return None
    b = lic_gdf.total_bounds  # [minx, miny, maxx, maxy]
    dx = (b[2] - b[0]) * margin
    dy = (b[3] - b[1]) * margin
    return (float(b[0] - dx), float(b[1] - dy), float(b[2] + dx), float(b[3] + dy))


def clip_to_view(
    gdf: gpd.GeoDataFrame,
    bounds: tuple[float, float, float, float],
) -> gpd.GeoDataFrame:
    """Обрезать GeoDataFrame по (minx, miny, maxx, maxy). Возвращает отфильтрованный слой."""
    from shapely.geometry import box as _box
    bbox = _box(*bounds)
    return gdf[gdf.intersects(bbox)].copy()


# ---------------------------------------------------------------------------
# Статистика и colorbar
# ---------------------------------------------------------------------------

def clip_quantiles(series, low: float = 0.02, high: float = 0.98) -> tuple[float, float]:
    """Обрезать выбросы по квантилям для colorbar. Возвращает (vmin, vmax)."""
    valid = series.dropna()
    if len(valid) == 0:
        return 0.0, 1.0
    return float(np.nanpercentile(valid, low * 100)), float(np.nanpercentile(valid, high * 100))


def field_stats(series) -> dict:
    """Вычислить базовую статистику по серии. Возвращает dict."""
    valid = series.dropna()
    if len(valid) == 0:
        return {}
    if np.issubdtype(series.dtype, np.number):
        return {
            "min": round(float(valid.min()), 6),
            "max": round(float(valid.max()), 6),
            "mean": round(float(valid.mean()), 6),
            "median": round(float(valid.median()), 6),
            "std": round(float(valid.std()), 6),
            "nulls": int(series.isna().sum()),
        }
    else:
        vc = valid.astype(str).value_counts()
        return {
            "unique_count": int(valid.nunique()),
            "top_values": {k: int(v) for k, v in vc.head(10).items()},
            "nulls": int(series.isna().sum()),
        }


# ---------------------------------------------------------------------------
# Заголовки
# ---------------------------------------------------------------------------

def make_title(layer_id: str, manifest: dict, field: str | None = None) -> str:
    """Собрать заголовок из display_name, units, field и feature_count.

    Пример: "Поле дельта G (мГал) — ID_123, n=102,216"
    """
    layers = {l["layer_id"]: l for l in manifest.get("layers", [])}
    entry = layers.get(layer_id, {})
    display_name = entry.get("display_name", layer_id)
    units = entry.get("units")
    feature_count = entry.get("feature_count", 0)

    title = display_name
    if units:
        title += f" ({units})"
    if field:
        title += f" — {field}"
    if feature_count:
        title += f", n={feature_count:,}"
    return title


def make_colorbar_label(field_name: str, units: str | None) -> str:
    """Подпись для colorbar: 'ID_123, мГал'"""
    return f"{field_name}, {units}" if units else field_name


# ---------------------------------------------------------------------------
# Автоподбор colormap
# ---------------------------------------------------------------------------

def auto_colormap(field_name: str | None, units: str | None, display_name: str | None) -> str:
    """Подбор colormap по семантике поля и единиц измерения."""
    import re
    units_str = units or ""
    # Если units не задан — извлечь из display_name: "Поле дельта G (мГал)" → "мГал"
    if not units_str and display_name:
        m = re.search(r'\(([^)]+)\)\s*$', display_name)
        if m:
            units_str = m.group(1)

    field_str = (field_name or "").lower()
    display_str = (display_name or "").lower()
    units_lower = units_str.lower()

    if "мгал" in units_lower or "э" in units_lower:
        return "RdYlBu_r"
    if "нтл" in units_lower:
        return "RdBu_r"
    if any(kw in field_str for kw in ("elev", "height", "abs", "phlr", "relief", "_z", "point_z")):
        return "terrain"
    if "градиент" in display_str:
        return "magma"
    return "viridis"


# ---------------------------------------------------------------------------
# Сохранение
# ---------------------------------------------------------------------------

def save_figure(fig: plt.Figure, project_id: str, name: str, fmt: str = "png", dpi: int = 150) -> str:
    """Сохранить фигуру в projects/{project_id}/viz/{name}.{fmt}. Вернуть путь."""
    from arcgis_mcp.config import PROJECTS_DIR
    viz_dir = Path(PROJECTS_DIR) / project_id / "viz"
    viz_dir.mkdir(parents=True, exist_ok=True)
    path = viz_dir / f"{name}.{fmt}"
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return str(path)


# ---------------------------------------------------------------------------
# MinIO — загрузка файлов визуализации в объектное хранилище
# ---------------------------------------------------------------------------

_minio_client = None
_bucket_ready: bool = False


def _get_minio():
    """Ленивый синглтон клиента MinIO. Возвращает None при недоступности."""
    global _minio_client
    if _minio_client is not None:
        return _minio_client
    try:
        from arcgis_mcp.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        from minio import Minio
        _minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
    except Exception:
        return None
    return _minio_client


def _ensure_bucket() -> bool:
    """Создать бакет и установить публичную политику чтения, если ещё не сделано."""
    global _bucket_ready
    if _bucket_ready:
        return True
    client = _get_minio()
    if client is None:
        return False
    try:
        import json as _json
        from arcgis_mcp.config import MINIO_BUCKET
        from minio.error import S3Error
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
        policy = _json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{MINIO_BUCKET}/*",
            }],
        })
        client.set_bucket_policy(MINIO_BUCKET, policy)
        _bucket_ready = True
        return True
    except Exception:
        return False


def upload_to_minio(local_path: str, project_id: str) -> str | None:
    """Загрузить файл в MinIO и вернуть публичный URL. None при ошибке."""
    if not _ensure_bucket():
        return None
    client = _get_minio()
    if client is None:
        return None
    try:
        from arcgis_mcp.config import MINIO_BUCKET, MINIO_PUBLIC_HOST
        filename = Path(local_path).name
        object_name = f"{project_id}/{filename}"
        client.fput_object(MINIO_BUCKET, object_name, local_path)
        return f"http://{MINIO_PUBLIC_HOST}/{MINIO_BUCKET}/{object_name}"
    except Exception:
        return None


# ---------------------------------------------------------------------------
# DEFAULT_STYLES для plot_overlay (геометрический фолбэк)
# ---------------------------------------------------------------------------

DEFAULT_STYLES: dict[str, dict] = {
    "Point":           {"color": "steelblue", "markersize": 8},
    "MultiPoint":      {"color": "steelblue", "markersize": 8},
    "LineString":      {"color": "steelblue", "linewidth": 1},
    "MultiLineString": {"color": "steelblue", "linewidth": 1},
    "Polygon":         {"color": "lightblue", "edgecolor": "gray", "alpha": 0.4},
    "MultiPolygon":    {"color": "lightblue", "edgecolor": "gray", "alpha": 0.4},
}


# ---------------------------------------------------------------------------
# Семантические стили — детерминированные цвета по типу слоя
# ---------------------------------------------------------------------------
# Формат: (layer_patterns, fd_patterns, style_dict)
# layer_patterns проверяются (substring, case-insensitive) в layer_id + display_name.
# fd_patterns проверяются в feature_dataset; пустой список = без ограничения по fd.
# Порядок важен — первый совпавший выигрывает.
# ---------------------------------------------------------------------------

_SEMANTIC_STYLE_RULES: list[tuple[list[str], list[str], dict]] = [
    # ----- Топооснова -----
    (["river", "реки"],                              [],           {"color": "#4488FF", "linewidth": 0.8, "alpha": 0.9}),
    (["lake", "озёр", "озер"],                       [],           {"color": "#87CEEB", "edgecolor": "#4488FF", "alpha": 0.5}),
    (["road", "дорог"],                              [],           {"color": "#888888", "linewidth": 0.5, "alpha": 0.8}),
    (["town", "насел", "settlement", "город"],       [],           {"color": "#8B4513", "markersize": 6}),
    (["relief", "горизонт", "contour", "рельеф"],    [],           {"color": "#A0785A", "linewidth": 0.4, "alpha": 0.7}),
    (["rama", "ramka", "frame", "рамк"],             [],           {"color": "#AAAAAA", "linewidth": 0.3, "alpha": 0.5}),
    (["obl_p", "border", "boundary", "адм"],         [],           {"color": "#666666", "linewidth": 0.6, "linestyle": "--"}),
    (["gridsheet", "grid"],                          ["grid"],     {"color": "#CCCCCC", "edgecolor": "#AAAAAA", "alpha": 0.3, "linewidth": 0.3}),
    # ----- Геофизика — изолинии -----
    (["izol", "изол", "n_pole", "нормальн"],         [],           {"color": "#BBBBBB", "linewidth": 0.3, "alpha": 0.6}),
    # ----- Геофизика — линеаменты -----
    (["lin", "lineament", "линеам"],                 [],           {"color": "#00BB44", "linewidth": 1.0}),
    # ----- Геофизика — экстремумы -----
    (["extr_pol", "положит"],                        [],           {"color": "#CC0000", "marker": "^", "markersize": 8}),
    (["extr_otr", "отрицат"],                        [],           {"color": "#0055CC", "marker": "v", "markersize": 8}),
    # ----- Геология — тектоника / разломы -----
    (["tect", "fault", "разлом", "разрывн", "наруш", "надвиг", "шарьяж"], ["geology"],
                                                                   {"color": "#1A1A1A", "linewidth": 1.2}),
    # ----- Геология — рудные точки -----
    (["drud", "ore", "руд", "пи"],                   [],           {"color": "#FFD700", "marker": "D", "markersize": 10, "edgecolor": "#333333"}),
    # ----- Геохимия / ореолы -----
    (["вторичн", "ореол"],                           ["geochem"],  {"color": "#FFB347", "edgecolor": "#CC7700", "alpha": 0.5}),
    # ----- Геология — полигоны (базовая геология, минерагения) -----
    (["geol", "геол", "basea", "mrana", "chema"],    ["geology"],  {"color": "#B8E8A0", "edgecolor": "#4A7A30", "alpha": 0.5}),
    # ----- Поисковые профили -----
    (["профил", "profile"],                          [],           {"color": "#FF8C00", "linewidth": 0.7}),
    # ----- Шурфы -----
    (["шурф"],                                       [],           {"color": "#8B4513", "marker": "s", "markersize": 7}),
    # ----- Скважины -----
    (["скважин", "well", "borehole"],                [],           {"color": "#FFD700", "marker": "o", "markersize": 8, "edgecolor": "#333333"}),
    # ----- Канавы / траншеи -----
    (["канав", "trench"],                            [],           {"color": "#8B4513", "linewidth": 1.0}),
    # ----- Изученность -----
    (["изучен", "izuch", "opmar", "survey"],         ["study"],    {"color": "#90EE90", "edgecolor": "#228B22", "alpha": 0.35}),
]


def get_semantic_style(
    layer_id: str,
    display_name: str,
    feature_dataset: str | None = None,
) -> dict | None:
    """Вернуть детерминированный стиль по семантике слоя.

    Возвращает dict со стилем (color, linewidth, marker, ...) или None если нет совпадения.
    Агент или пользователь могут переопределить любое поле явно в spec.
    """
    search = f"{layer_id} {display_name}".lower()
    fd_lower = (feature_dataset or "").lower()
    for layer_patterns, fd_patterns, style in _SEMANTIC_STYLE_RULES:
        layer_match = any(p in search for p in layer_patterns)
        fd_match = (not fd_patterns) or any(p in fd_lower for p in fd_patterns)
        if layer_match and fd_match:
            return style
    return None


# ---------------------------------------------------------------------------
# Автоматические tooltip-поля для folium
# ---------------------------------------------------------------------------

_SKIP_TOOLTIP = {"geometry", "OBJECTID", "Shape_Length", "Shape_Area", "GLOBALID", "Shape"}

def auto_tooltip_fields(gdf: gpd.GeoDataFrame, manifest_layer: dict) -> list[str]:
    """Выбрать до 5 полей для tooltip автоматически."""
    fields: list[str] = []

    # display_field из manifest
    df = manifest_layer.get("display_field") or manifest_layer.get("aprx_display_field")
    if df and df in gdf.columns:
        fields.append(df)

    # Поля с именами/названиями
    for col in gdf.columns:
        if col.lower() in ("имя", "name", "название", "name_otch", "vid_iz") and col not in fields:
            fields.append(col)

    # Первые информативные поля (не системные)
    for col in gdf.columns:
        if col not in _SKIP_TOOLTIP and col not in fields and col != "geometry":
            fields.append(col)
        if len(fields) >= 5:
            break

    return fields
