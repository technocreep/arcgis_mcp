from __future__ import annotations

from typing import List, Dict

from _backend.projects_utils.project_store import ProjectStore
PROJECTS_DIR = "/run/media/karl/BIG_SSD/arcgis_mcp/_refactored/_backend/database/projects"

def load_projects(projects_dir: str | None = None) -> List[Dict]:
    """Load projects from PROJECTS_DIR using ProjectStore and return a
    serializable list of dicts with fields id, title, layers_count.

    The function defaults to the main app's PROJECTS_DIR so both servers
    read the same index.
    """
    dir_to_use = projects_dir or str(PROJECTS_DIR)
    store = ProjectStore(str(dir_to_use))
    summaries = store.list_projects()
    result = []
    for p in summaries:
        entry = {
            "id": p.id,
            "name": p.name,
            "created_at": p.created_at,
            "layers_count": p.layers_count,
            "has_attachments": p.has_attachments,
            "gdb_file": p.gdb_file,
            "primary_crs": None,
            "metadata_completeness": None,
        }
        # Try to enrich with manifest fields if available
        try:
            manifest = store.get_manifest(p.id)
            proj = manifest.get("project", {}) if isinstance(manifest, dict) else {}
            # common keys that might exist
            entry["primary_crs"] = proj.get("primary_crs") or proj.get("crs") or proj.get("source_crs")
            entry["metadata_completeness"] = proj.get("metadata_completeness") or manifest.get("metadata_completeness")
        except Exception:
            # ignore manifest errors (missing manifest)
            pass
        result.append(entry)

    return result
