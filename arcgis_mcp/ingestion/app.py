"""Ingestion API — веб-сервис для загрузки и управления проектами.

Запуск:
    uvicorn ingestion.app:app --reload
"""

import shutil
import zipfile
import os
from pathlib import Path
from typing import List
import tempfile

import secrets

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Импорты из проекта
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import AUTH_PASSWORD, AUTH_USERNAME, PROJECTS_DIR
from ingestion.pipeline import run_pipeline
from mcp_server.project_store import ProjectStore

app = FastAPI(title="GIS Ingestion API", version="1.0")

# ---------------------------------------------------------------------------
# BasicAuth — защищает write-эндпоинты (upload, delete)
# Учётные данные задаются через GIS_USERNAME / GIS_PASSWORD (см. config.py)
# ---------------------------------------------------------------------------
_security = HTTPBasic()

def require_auth(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
    ok = (
        secrets.compare_digest(credentials.username.encode(), AUTH_USERNAME.encode())
        and secrets.compare_digest(credentials.password.encode(), AUTH_PASSWORD.encode())
    )
    if not ok:
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# CORS (для разработки)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Статика (Frontend)
static_dir = Path(__file__).parent.parent / "static"
# static_dir.mkdir(exist_ok=True)
app.mount("/ui", StaticFiles(directory=str(static_dir), html=True), name="static")

store = ProjectStore(PROJECTS_DIR)

@app.get("/")
async def root():
    return FileResponse(static_dir / "index.html")

@app.get("/api/projects")
async def list_projects():
    """Список доступных проектов."""
    return {"projects": [p.__dict__ for p in store.list_projects()]}

@app.get("/api/projects/{project_id}")
async def get_project(project_id: str):
    """Получить манифест проекта."""
    try:
        return store.get_manifest(project_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Project not found")

@app.delete("/api/projects/{project_id}")
async def delete_project(project_id: str, _: str = Depends(require_auth)):
    """Удалить проект."""
    project_path = Path(PROJECTS_DIR) / project_id
    if not project_path.exists():
        raise HTTPException(status_code=404, detail="Project not found")
    
    shutil.rmtree(project_path)
    
    # Обновляем индекс (грубый метод, лучше вынести логику в store)
    # В реальном приложении pipeline._update_index должен уметь удалять
    index_path = Path(PROJECTS_DIR) / "_index.json"
    if index_path.exists():
        import json
        data = json.loads(index_path.read_text(encoding="utf-8"))
        data["projects"] = [p for p in data.get("projects", []) if p.get("id") != project_id]
        index_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        
    return {"status": "deleted", "project_id": project_id}

@app.post("/api/upload")
async def upload_project(
    project_id: str = Form(...),
    gdb_zip: UploadFile = File(..., description="Архив .zip содержащий .gdb папку"),
    aprx: UploadFile = File(None, description="Файл проекта .aprx"),
    atbx: UploadFile = File(None, description="Файл тулбокса .atbx"),
    _: str = Depends(require_auth),
):
    """Загрузка нового проекта.
    
    1. Принимает файлы во временную папку.
    2. Распаковывает GDB.
    3. Запускает Ingestion Pipeline.
    """
    
    # Проверка ID
    if (Path(PROJECTS_DIR) / project_id).exists():
        raise HTTPException(status_code=400, detail=f"Project '{project_id}' already exists")

    temp_dir = Path(tempfile.mkdtemp(prefix="gis_ingest_"))
    
    try:
        # 1. Сохраняем GDB Zip
        gdb_zip_path = temp_dir / gdb_zip.filename
        with open(gdb_zip_path, "wb") as buffer:
            shutil.copyfileobj(gdb_zip.file, buffer)
            
        # 2. Распаковка GDB
        extract_path = temp_dir / "extracted_gdb"
        extract_path.mkdir()
        with zipfile.ZipFile(gdb_zip_path, "r") as zf:
            zf.extractall(extract_path)
            
        # Ищем .gdb папку внутри
        found_gdb = None
        for root, dirs, files in os.walk(extract_path):
            for d in dirs:
                if d.lower().endswith(".gdb"):
                    found_gdb = Path(root) / d
                    break
            if found_gdb:
                break
        
        if not found_gdb:
             raise HTTPException(status_code=400, detail="No .gdb folder found inside the zip archive")

        # 3. Сохраняем APRX (если есть)
        aprx_path = None
        if aprx:
            aprx_path = temp_dir / aprx.filename
            with open(aprx_path, "wb") as buffer:
                shutil.copyfileobj(aprx.file, buffer)

        # 4. Сохраняем ATBX (если есть)
        atbx_path = None
        if atbx:
            atbx_path = temp_dir / atbx.filename
            with open(atbx_path, "wb") as buffer:
                shutil.copyfileobj(atbx.file, buffer)

        # 5. Запуск пайплайна
        # В продакшене это должно быть в BackgroundTasks, но для UI удобнее дождаться результата
        try:
            manifest = run_pipeline(
                gdb_path=found_gdb,
                aprx_path=aprx_path,
                atbx_path=atbx_path,
                project_id=project_id,
                output_dir=Path(PROJECTS_DIR),
                verbose=True
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

        return {
            "status": "success",
            "project_id": project_id,
            "layers_count": manifest["quality"]["layers_total"],
            "mapping_coverage": manifest["mapping_quality"]["coverage_percent"]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)