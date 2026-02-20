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
    units_str = units or ""
    field_str = (field_name or "").lower()
    display_str = (display_name or "").lower()

    if "мГал" in units_str or "Э" in units_str:
        return "RdYlBu_r"
    if "нТл" in units_str:
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
# DEFAULT_STYLES для plot_overlay
# ---------------------------------------------------------------------------

DEFAULT_STYLES: dict[str, dict] = {
    "Point":           {"color": "red",       "markersize": 10},
    "MultiPoint":      {"color": "red",       "markersize": 10},
    "LineString":      {"color": "steelblue", "linewidth": 1},
    "MultiLineString": {"color": "steelblue", "linewidth": 1},
    "Polygon":         {"color": "lightblue", "edgecolor": "gray", "alpha": 0.4},
    "MultiPolygon":    {"color": "lightblue", "edgecolor": "gray", "alpha": 0.4},
}


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
