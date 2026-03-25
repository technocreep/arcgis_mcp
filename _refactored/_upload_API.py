from fastapi import APIRouter, Request, Depends, HTTPException
from pathlib import Path
import tempfile
import shutil
import zipfile
import json
from datetime import datetime

from _backend.auth_utils.auth_utils import verify_user
from _backend.projects_utils.load_projects import PROJECTS_DIR
from _backend.upload import save_and_extract_project
from _backend.indexer import add_or_update_project

router = APIRouter()


@router.post("/upload")
async def upload_proxy(request: Request, user=Depends(verify_user)):
    """Handle project upload: save files, extract GDB, and update _index.json.

    This handler stores uploaded files and updates the projects index so the
    frontend can immediately list the new project. It does not run the
    ingestion pipeline.
    """
    form = await request.form()
    project_id = form.get('project_id')
    aprx = form.get('aprx')
    atbx = form.get('atbx')

    if not project_id:
        raise HTTPException(status_code=422, detail='Missing project_id')

    # Basic validation of project id
    if '/' in project_id or '\\' in project_id or project_id.strip() == '':
        raise HTTPException(status_code=400, detail='Invalid project_id')

    # Collect archives from form (support multiple parts named 'gdb_zip' or 'archive')
    archives = []
    for k, v in form.multi_items():
        if k in ('gdb_zip', 'archive', 'archives', 'file'):
            archives.append(v)

    if not archives:
        raise HTTPException(status_code=422, detail='No archive files provided')

    try:
        result = save_and_extract_project(PROJECTS_DIR, project_id, archives, aprx=aprx, atbx=atbx)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Build index entry and add to _index.json via indexer
    entry = {
        'id': project_id,
        'name': project_id,
        'created_at': datetime.utcnow().isoformat() + 'Z',
        'layers_count': 0,
        'has_attachments': bool(aprx or atbx),
        'gdb_file': result.get('gdb_name'),
    }
    try:
        add_or_update_project(PROJECTS_DIR, entry)
    except Exception as e:
        # Cleanup project folder on failure to update index
        try:
            p = Path(PROJECTS_DIR) / project_id
            if p.exists():
                shutil.rmtree(p)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f'Failed to update index: {e}')

    return {'status': 'success', 'project_id': project_id}
