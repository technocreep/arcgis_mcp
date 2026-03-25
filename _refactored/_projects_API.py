from fastapi import APIRouter, Depends, Request, HTTPException

from pydantic import BaseModel

from _backend.auth_utils.auth_utils import verify_user

from _backend.projects_utils.load_projects import load_projects, PROJECTS_DIR
from _backend.projects_utils.project_store import ProjectStore
import math


def _sanitize_for_json(obj):
    """Recursively replace non-JSON-compliant numeric values (NaN, +Inf, -Inf)
    with None and convert numpy-like numbers to native Python primitives.

    This prevents ValueError from the JSON encoder when the manifest contains
    NaN/Inf values.
    """
    # dict
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    # list/tuple
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    # basic primitives
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    # floats and numeric-like
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    # try to coerce numpy scalars and other numerics to float
    try:
        f = float(obj)
    except Exception:
        # fallback to string for unknown objects
        try:
            return str(obj)
        except Exception:
            return None
    else:
        return float(f) if math.isfinite(f) else None


router = APIRouter(prefix="/projects", tags=["projects"], dependencies=[])


class ProjectOut(BaseModel):
    id: str
    name: str
    created_at: str | None = None
    layers_count: int | None = None
    has_attachments: bool | None = None
    gdb_file: str | None = None
    primary_crs: str | None = None
    metadata_completeness: str | None = None


@router.get("/", response_model=list[ProjectOut])
async def list_projects(request: Request, user=Depends(verify_user)):
    """Return list of projects read from ProjectStore._index.json.

    Mirrors the `list_projects` implementation used in `arcgis_mcp/api_server/server.py`.
    """
    return load_projects()


@router.get("/{project_id}")
async def get_project(project_id: str, request: Request, user=Depends(verify_user)):
    """Return project summary and full manifest for a single project.

    Response shape matches what the front-end `ProjectDetail` expects:
    { summary: {...}, manifest: {...} }
    """
    summaries = load_projects()
    summary = next((p for p in summaries if p.get("id") == project_id), None)
    if summary is None:
        raise HTTPException(status_code=404, detail="Project not found")

    store = ProjectStore(PROJECTS_DIR)
    try:
        manifest = store.get_manifest(project_id)
    except Exception:
        manifest = None

    # Sanitize manifest to avoid JSON serialization errors (NaN/Inf)
    if manifest is not None:
        manifest = _sanitize_for_json(manifest)

    return {"summary": summary, "manifest": manifest}
