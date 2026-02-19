"""Расчёт quality score для GIS-проекта.

Оценивает полноту и качество данных после ingestion:
- coverage маппинга .aprx → .gdb
- наличие display_names, описаний
- консистентность CRS
- наличие вложений и их извлекаемость
- наличие 3D слоёв и растров
"""

from __future__ import annotations

from dataclasses import dataclass

from .mapping import LayerMappingResult
from .parser_gdb import GdbData


@dataclass
class QualityReport:
    layers_total: int
    layers_non_empty: int
    layers_with_display_name: int    # source == "aprx"
    layers_with_unknown_meaning: int # source == "gdb_only"
    attachments_extractable: bool
    crs_consistent: bool
    primary_crs: str | None
    has_3d_layers: bool
    has_rasters: bool
    metadata_completeness: str       # "low" / "medium" / "high"
    coverage_percent: float
    warnings: list[str]


_3D_GEOMETRY_TYPES = {
    "3D Point", "3D MultiPoint", "3D LineString", "3D MultiLineString",
    "3D Polygon", "3D MultiPolygon", "MultiPatchZ",
}

_RASTER_KEYWORDS = {"raster", "mosaic", "image"}


def compute_quality(
    gdb_data: GdbData,
    mapping: LayerMappingResult,
) -> QualityReport:
    """Вычислить quality report по результатам ingestion.

    Args:
        gdb_data: результат parse_gdb()
        mapping:  результат build_mapping()

    Returns:
        QualityReport
    """
    non_attach_layers = [lp for lp in gdb_data.layers if not lp.is_attachment_table]
    layers_total = len(non_attach_layers)
    layers_non_empty = sum(1 for lp in non_attach_layers if lp.feature_count > 0)

    # Display names
    layers_with_display_name = sum(
        1 for m in mapping.mapped if m.display_name_source in ("aprx", "dict", "inferred")
    )
    layers_with_unknown_meaning = sum(
        1 for m in mapping.mapped if m.needs_review
    )

    # Вложения
    attachments_extractable = len(gdb_data.attachment_tables) > 0

    # CRS-консистентность: все слои с геометрией должны иметь одинаковый EPSG
    epsg_values = {
        lp.crs_epsg
        for lp in non_attach_layers
        if lp.crs_epsg is not None and lp.geometry_type is not None
    }
    crs_consistent = len(epsg_values) <= 1
    primary_crs: str | None = None
    if epsg_values:
        # берём самый частый EPSG
        from collections import Counter
        epsg_counter = Counter(
            lp.crs_epsg
            for lp in non_attach_layers
            if lp.crs_epsg is not None and lp.geometry_type is not None
        )
        most_common_epsg = epsg_counter.most_common(1)[0][0]
        primary_crs = f"EPSG:{most_common_epsg}"

    # 3D слои
    has_3d_layers = any(
        lp.geometry_type and any(kw in lp.geometry_type for kw in ("3D", "Z", "Patch"))
        for lp in non_attach_layers
    )

    # Растры
    has_rasters = any(
        any(kw in (lp.layer_id or "").lower() for kw in _RASTER_KEYWORDS)
        for lp in non_attach_layers
    )

    # Metadata completeness
    # "high": >80% с display_name + >50% с description
    # "medium": >50% с display_name
    # "low": остальное
    coverage = mapping.quality.coverage_percent
    if layers_total > 0:
        descriptions_count = sum(
            1 for m in mapping.mapped if m.description and m.description != m.dataset_name
        )
        desc_ratio = descriptions_count / layers_total

        if coverage >= 80 and desc_ratio >= 0.5:
            metadata_completeness = "high"
        elif coverage >= 50:
            metadata_completeness = "medium"
        else:
            metadata_completeness = "low"
    else:
        metadata_completeness = "low"

    return QualityReport(
        layers_total=layers_total,
        layers_non_empty=layers_non_empty,
        layers_with_display_name=layers_with_display_name,
        layers_with_unknown_meaning=layers_with_unknown_meaning,
        attachments_extractable=attachments_extractable,
        crs_consistent=crs_consistent,
        primary_crs=primary_crs,
        has_3d_layers=has_3d_layers,
        has_rasters=has_rasters,
        metadata_completeness=metadata_completeness,
        coverage_percent=coverage,
        warnings=mapping.warnings[:],
    )
