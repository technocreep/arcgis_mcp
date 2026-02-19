"""Ingestion pipeline — точка входа для Фазы 1.

Запуск:
    python -m ingestion.pipeline --gdb path/to/data.gdb --aprx path/to/data.aprx \
                                  --project-id my-project [--output ./projects]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import zipfile
from pathlib import Path

# Добавляем корень проекта в путь (нужно при запуске как скрипта)
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.manifest_builder import build_manifest
from ingestion.mapping import build_mapping
from ingestion.parser_aprx import parse_aprx
from ingestion.parser_gdb import parse_gdb
from ingestion.quality import compute_quality


def run_pipeline(
    gdb_path: Path,
    aprx_path: Path | None,
    project_id: str,
    output_dir: Path,
    atbx_path: Path | None = None,
    verbose: bool = True,
) -> dict:
    """Запустить полный ingestion pipeline.

    Returns:
        Готовый manifest dict
    """

    def log(msg: str):
        if verbose:
            print(f"[pipeline] {msg}")

    log(f"Проект: {project_id}")
    log(f"GDB: {gdb_path}")
    log(f"APRX: {aprx_path}")

    # --- Шаг 1: Парсинг .aprx ---
    aprx_data = None
    if aprx_path and aprx_path.exists():
        log("Шаг 1: Парсинг .aprx...")
        aprx_data = parse_aprx(aprx_path)
        log(f"  Слоёв в .aprx: {len(aprx_data.layer_mappings)}")
        log(f"  Групп: {len(aprx_data.groups)}")
        log(f"  Карта: {aprx_data.map_name!r}")
    else:
        log("Шаг 1: .aprx не предоставлен — все слои будут gdb_only")

    # --- Шаг 2: Парсинг .gdb ---
    log("Шаг 2: Парсинг .gdb...")
    gdb_data = parse_gdb(gdb_path)
    non_attach = [lp for lp in gdb_data.layers if not lp.is_attachment_table]
    log(f"  Слоёв в .gdb: {len(non_attach)}")
    log(f"  Таблиц вложений: {len(gdb_data.attachment_tables)}")

    # --- Шаг 3: Маппинг ---
    log("Шаг 3: Построение маппинга...")
    mapping = build_mapping(aprx_data, gdb_data)
    log(f"  Замаплено: {mapping.quality.mapped_from_aprx}/{mapping.quality.total_gdb_layers} "
        f"({mapping.quality.coverage_percent}%)")
    if mapping.warnings:
        for w in mapping.warnings:
            log(f"  WARN: {w}")

    # --- Шаг 4: Quality score ---
    log("Шаг 4: Расчёт quality score...")
    quality = compute_quality(gdb_data, mapping)
    log(f"  Completeness: {quality.metadata_completeness}")
    log(f"  CRS consistent: {quality.crs_consistent}, primary: {quality.primary_crs}")

    # --- Шаг 5: Сборка manifest ---
    log("Шаг 5: Сборка manifest.json...")
    source_files = {
        "gdb": gdb_path.name,
        "aprx": aprx_path.name if aprx_path else None,
        "atbx": atbx_path.name if atbx_path else None,
    }
    manifest = build_manifest(
        project_id=project_id,
        gdb_data=gdb_data,
        aprx_data=aprx_data,
        mapping=mapping,
        quality=quality,
        source_files=source_files,
    )

    # --- Шаг 6: Сохранение ---
    log("Шаг 6: Сохранение файлов...")
    project_dir = output_dir / project_id
    project_dir.mkdir(parents=True, exist_ok=True)

    # manifest.json
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"  -> {manifest_path}")

    # layer_mapping.json (отдельно для отладки)
    layer_mapping_path = project_dir / "layer_mapping.json"
    layer_mapping_path.write_text(
        json.dumps({
            "layer_mapping": manifest["layer_mapping"],
            "unmapped_layers": manifest["unmapped_layers"],
            "mapping_quality": manifest["mapping_quality"],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"  -> {layer_mapping_path}")

    # layer_profiles/*.json — детальные профили слоёв из .gdb
    profiles_dir = project_dir / "layer_profiles"
    profiles_dir.mkdir(exist_ok=True)
    lp_index = {lp.layer_id: lp for lp in gdb_data.layers}
    for layer_entry in manifest["layers"]:
        ds_name = layer_entry["layer_id"]
        lp = lp_index.get(ds_name)
        if lp is None:
            continue
        profile_data = {
            "layer_id": lp.layer_id,
            "geometry_type": lp.geometry_type,
            "feature_count": lp.feature_count,
            "crs_epsg": lp.crs_epsg,
            "crs_wkt": lp.crs_wkt,
            "extent_native": lp.extent_native,
            "extent_wgs84": lp.extent_wgs84,
            "is_large": lp.is_large,
            "fields": [
                {
                    "name": f.name,
                    "dtype": f.dtype,
                    "nulls": f.nulls,
                    "min": f.min, "max": f.max, "mean": f.mean, "std": f.std,
                    "unique_count": f.unique_count,
                    "top_values": f.top_values,
                }
                for f in lp.fields
            ],
        }
        safe_name = ds_name.replace("/", "_").replace("\\", "_")
        profile_path = profiles_dir / f"{safe_name}.json"
        profile_path.write_text(
            json.dumps(profile_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # aprx_unpacked/ — сохранение JSON из .aprx
    if aprx_path and aprx_path.exists():
        aprx_dir = project_dir / "aprx_unpacked"
        aprx_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(aprx_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith(".json"):
                    target = aprx_dir / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(zf.read(name))
        log(f"  -> {aprx_dir}/")

    # _index.json — реестр проектов для MCP server
    _update_index(output_dir, project_id, manifest)
    log(f"  -> {output_dir / '_index.json'}")

    log("Готово!")
    return manifest


def _update_index(output_dir: Path, project_id: str, manifest: dict):
    """Обновить реестр проектов _index.json."""
    index_path = output_dir / "_index.json"
    if index_path.exists():
        try:
            index = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            index = {"projects": []}
    else:
        index = {"projects": []}

    # Удалить старую запись если есть
    index["projects"] = [p for p in index["projects"] if p.get("id") != project_id]

    proj = manifest.get("project", {})
    quality = manifest.get("quality", {})
    attach = manifest.get("attachments_summary", {})

    index["projects"].append({
        "id": project_id,
        "name": proj.get("name", project_id),
        "created_at": manifest.get("generated_at", ""),
        "layers_count": quality.get("layers_total", 0),
        "has_attachments": attach.get("total", 0) > 0,
        "gdb_file": proj.get("source_files", {}).get("gdb"),
        "primary_crs": quality.get("primary_crs"),
        "metadata_completeness": quality.get("metadata_completeness"),
    })

    index_path.write_text(
        json.dumps(index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="GIS Ingestion Pipeline — Фаза 1")
    parser.add_argument("--gdb", required=True, help="Путь к .gdb")
    parser.add_argument("--aprx", help="Путь к .aprx (опционально)")
    parser.add_argument("--atbx", help="Путь к .atbx (опционально)")
    parser.add_argument("--project-id", required=True, help="Идентификатор проекта (slug)")
    parser.add_argument("--output", default="projects", help="Директория для хранения проектов")
    parser.add_argument("--quiet", action="store_true", help="Тихий режим")
    args = parser.parse_args()

    manifest = run_pipeline(
        gdb_path=Path(args.gdb),
        aprx_path=Path(args.aprx) if args.aprx else None,
        project_id=args.project_id,
        output_dir=Path(args.output),
        atbx_path=Path(args.atbx) if args.atbx else None,
        verbose=not args.quiet,
    )

    # Краткий итог
    print(f"\n=== Итог ===")
    print(f"Проект:   {manifest['project']['id']}")
    print(f"Карта:    {manifest['project']['map'].get('name', '—')}")
    print(f"Слоёв:    {manifest['quality']['layers_total']}")
    print(f"Маппинг:  {manifest['mapping_quality']['coverage_percent']}%")
    print(f"Quality:  {manifest['quality']['metadata_completeness']}")
    if manifest["quality"]["warnings"]:
        print(f"Warnings: {len(manifest['quality']['warnings'])}")


if __name__ == "__main__":
    main()
