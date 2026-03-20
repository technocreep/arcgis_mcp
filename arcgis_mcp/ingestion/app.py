"""Ingestion API — веб-сервис для загрузки и управления проектами.

Запуск:
    uvicorn ingestion.app:app --reload
"""

import base64
import csv
import io
import json
import logging
import math
import shutil
import zipfile
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=__import__("sys").stdout,
)

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import tempfile

import secrets

import httpx
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
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
_security_optional = HTTPBasic(auto_error=False)

# ── Session store (in-memory, 8 h TTL) ──────────────────────────────────────
_sessions: dict[str, datetime] = {}
_SESSION_TTL = 8 * 3600  # seconds

def _new_session() -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = datetime.utcnow() + timedelta(seconds=_SESSION_TTL)
    return token

def _valid_session(token: str | None) -> bool:
    if not token:
        return False
    expiry = _sessions.get(token)
    if expiry and datetime.utcnow() < expiry:
        return True
    _sessions.pop(token, None)
    return False

def _check_credentials(username: str, password: str) -> bool:
    return (
        secrets.compare_digest(username.encode(), AUTH_USERNAME.encode())
        and secrets.compare_digest(password.encode(), AUTH_PASSWORD.encode())
    )

def require_auth(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
    if not _check_credentials(credentials.username, credentials.password):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def require_auth_flexible(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(_security_optional),
) -> str:
    """Auth via Basic header, ?_auth= query param, or gis_session cookie."""
    if _valid_session(request.cookies.get("gis_session")):
        return "session"
    if credentials and _check_credentials(credentials.username, credentials.password):
        return credentials.username
    auth_param = request.query_params.get("_auth")
    if auth_param:
        try:
            decoded = base64.b64decode(auth_param).decode("utf-8")
            username, password = decoded.split(":", 1)
            if _check_credentials(username, password):
                return username
        except Exception:
            pass
    raise HTTPException(
        status_code=401,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Basic"},
    )


def _resolve_auth(request: Request, credentials: Optional[HTTPBasicCredentials]) -> tuple[str | None, bool]:
    """Returns (username_or_None, should_set_session_cookie)."""
    if _valid_session(request.cookies.get("gis_session")):
        return "session", False
    if credentials and _check_credentials(credentials.username, credentials.password):
        return credentials.username, True
    auth_param = request.query_params.get("_auth")
    if auth_param:
        try:
            decoded = base64.b64decode(auth_param).decode("utf-8")
            username, password = decoded.split(":", 1)
            if _check_credentials(username, password):
                return username, True
        except Exception:
            pass
    return None, False

# CORS (для разработки)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Статические UI-файлы не кэшировать — достаточно обычного обновления страницы
@app.middleware("http")
async def no_cache_ui(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/ui/"):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response

# Статика (Frontend)
static_dir = Path(__file__).parent.parent / "static"
# static_dir.mkdir(exist_ok=True)
app.mount("/ui", StaticFiles(directory=str(static_dir), html=True), name="static")

store = ProjectStore(PROJECTS_DIR)

@app.get("/")
async def root():
    return FileResponse(static_dir / "index.html")

@app.get("/api/auth")
async def verify_auth(user: str = Depends(require_auth)):
    """Проверить учётные данные (используется фронтендом при логине)."""
    return {"ok": True, "user": user}


@app.get("/api/projects")
async def list_projects():
    """Список доступных проектов."""
    return {"projects": [p.__dict__ for p in store.list_projects()]}

def _sanitize(obj):
    """Заменить NaN/Inf на None — стандартный JSON не поддерживает эти значения."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str):
    """Получить манифест проекта."""
    try:
        data = store.get_manifest(project_id)
        return Response(
            content=json.dumps(_sanitize(data), ensure_ascii=False),
            media_type="application/json",
        )
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
        
    # Очистить данные проекта из Knowledge Graph (некритично)
    try:
        import config as _cfg
        if _cfg.NEO4J_URI:
            from rag.kg_client import Neo4jClient
            from rag.kg_builder import delete_project_subgraph
            kg = Neo4jClient(_cfg.NEO4J_URI, _cfg.NEO4J_USER, _cfg.NEO4J_PASSWORD)
            delete_project_subgraph(project_id, kg)
            kg.close()
    except Exception as _e:
        print(f"[delete] WARN: не удалось очистить KG для {project_id}: {_e}")

    return {"status": "deleted", "project_id": project_id}


@app.post("/api/projects/{project_id}/kg/build")
async def kg_build_project(project_id: str, _: str = Depends(require_auth)):
    """Построить / перестроить KG для уже загруженного проекта.

    Читает manifest.json с диска, индексирует Project/Layer/Field/Attachment
    и PDF карточки изученности. Идемпотентен (использует MERGE).
    """
    project_path = Path(PROJECTS_DIR) / project_id
    manifest_path = project_path / "manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Project or manifest not found")

    import config as _cfg
    if not _cfg.NEO4J_URI:
        raise HTTPException(status_code=503, detail="NEO4J_URI not configured")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gdb_name = manifest.get("project", {}).get("source_files", {}).get("gdb", "")
    gdb_path = str(project_path / "data" / gdb_name) if gdb_name else ""

    from rag.kg_client import Neo4jClient
    from rag.kg_builder import build_from_manifest, index_pdf_attachments, update_datacube_blocks

    kg = Neo4jClient(_cfg.NEO4J_URI, _cfg.NEO4J_USER, _cfg.NEO4J_PASSWORD)
    try:
        build_from_manifest(manifest, project_id, kg)
        if gdb_path and Path(gdb_path).exists():
            index_pdf_attachments(project_id, gdb_path, manifest, kg)

        # Индексировать DatacubeBlock узлы если артефакты есть на диске
        dc_dir = _find_dc_dir(project_path)
        if dc_dir is not None:
            logger.info("[KG] Datacube артефакты найдены: %s", dc_dir)
            artifacts = _load_datacube_artifacts(dc_dir)
            update_datacube_blocks(project_id, artifacts, kg)
        else:
            logger.info("[KG] Datacube артефакты не найдены для проекта %s, пропуск", project_id)

        rows = kg.execute(
            "MATCH (l:Layer {project_id: $pid}) RETURN count(l) AS n",
            {"pid": project_id},
        )
        dc_rows = kg.execute(
            "MATCH (b:DatacubeBlock {project_id: $pid}) RETURN count(b) AS n",
            {"pid": project_id},
        )
        return {
            "ok": True,
            "layers_indexed": rows[0]["n"] if rows else 0,
            "datacube_blocks_indexed": dc_rows[0]["n"] if dc_rows else 0,
        }
    finally:
        kg.close()


def _load_datacube_artifacts(dc_dir: Path) -> dict:
    """Прочитать CSV-артефакты Data Cube и вернуть dict для update_datacube_blocks."""

    def _read_csv(path: Path) -> list[dict]:
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for row in reader:
            clean = {}
            for k, v in row.items():
                v = v.strip()
                if v in ("", "nan", "NaN", "None", "null"):
                    clean[k] = None
                else:
                    try:
                        clean[k] = float(v) if "." in v or "e" in v.lower() else int(v)
                    except ValueError:
                        clean[k] = v
            rows.append(clean)
        return rows

    blocks = _read_csv(dc_dir / "blocks.csv")
    scores = _read_csv(dc_dir / "scores.csv")
    dominant_drivers = _read_csv(dc_dir / "interpretability" / "dominant_driver_group.csv")

    logger.info(
        "[KG] Datacube CSV: blocks=%d scores=%d dominant_drivers=%d",
        len(blocks), len(scores), len(dominant_drivers),
    )
    if not blocks:
        logger.warning("[KG] blocks.csv пуст или не найден в %s", dc_dir)
    if not scores:
        logger.warning("[KG] scores.csv пуст или не найден в %s", dc_dir)

    # update_datacube_blocks ожидает: block_id, dominant_driver, dominant_driver_group
    # dominant_driver_group.csv содержит колонки: block_id, dominant_driver, dominant_driver_group
    return {"blocks": blocks, "scores": scores, "dominant_drivers": dominant_drivers}


@app.post("/api/kg/cleanup")
async def kg_cleanup(_: str = Depends(require_auth)):
    """Удалить из KG узлы проектов, которых нет на диске.

    Сравнивает Project-узлы в Neo4j с папками в PROJECTS_DIR.
    Полезно для очистки «исторических» данных от проектов,
    удалённых до появления KG-cleanup в delete_project.
    """
    import config as _cfg
    if not _cfg.NEO4J_URI:
        return {"skipped": True, "reason": "NEO4J_URI not configured"}

    from rag.kg_client import Neo4jClient
    from rag.kg_builder import delete_project_subgraph

    kg = Neo4jClient(_cfg.NEO4J_URI, _cfg.NEO4J_USER, _cfg.NEO4J_PASSWORD)
    try:
        rows = kg.execute("MATCH (p:Project) RETURN p.id AS id")
        kg_ids = {r["id"] for r in rows if r.get("id")}

        disk_ids = {
            d.name for d in Path(PROJECTS_DIR).iterdir()
            if d.is_dir() and not d.name.startswith("_")
        }

        stale = kg_ids - disk_ids
        for pid in stale:
            delete_project_subgraph(pid, kg)
            print(f"[kg/cleanup] Удалён стale-проект: {pid}")

        return {"cleaned": sorted(stale), "count": len(stale)}
    finally:
        kg.close()


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


# ---------------------------------------------------------------------------
# Data Cube — прокси к сервису data-cube
# ---------------------------------------------------------------------------

DATACUBE_URL = os.getenv("DATACUBE_URL", "http://data-cube-server:8000")


@app.post("/api/datacube/jobs")
async def datacube_create_job(request: Request):
    """Запустить пайплайн Data Cube для проекта."""
    body = await request.json()
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{DATACUBE_URL}/jobs", json=body, timeout=10)
        return Response(content=r.content, status_code=r.status_code, media_type="application/json")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Data Cube service unavailable")


@app.get("/api/datacube/jobs/{job_id}")
async def datacube_get_job(job_id: str):
    """Получить статус и прогресс задачи Data Cube."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{DATACUBE_URL}/jobs/{job_id}", timeout=10)
        return Response(content=r.content, status_code=r.status_code, media_type="application/json")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Data Cube service unavailable")


# ---------------------------------------------------------------------------
# Data Cube — артефакты (файлы результатов пайплайна)
# ---------------------------------------------------------------------------

def _find_dc_dir(project_path: Path) -> Path | None:
    """Найти директорию с артефактами Data Cube (datacube/ или корень проекта)."""
    dc_sub = project_path / "datacube"
    for indicator in ("scores.csv", "blocks.csv"):
        if (dc_sub / indicator).exists():
            return dc_sub
        if (project_path / indicator).exists():
            return project_path
    return None


@app.get("/api/projects/{project_id}/datacube")
async def datacube_artifacts_status(project_id: str, _: str = Depends(require_auth)):
    """Проверить наличие артефактов Data Cube для проекта."""
    project_path = Path(PROJECTS_DIR) / project_id
    if not project_path.exists():
        raise HTTPException(status_code=404, detail="Project not found")

    dc_dir = _find_dc_dir(project_path)
    if dc_dir is None:
        return {"exists": False}

    files = [f.relative_to(dc_dir).as_posix() for f in dc_dir.rglob("*") if f.is_file()]
    return {"exists": True, "files": sorted(files)}


_DASHBOARD_HEAD_INJECT = (
    '<link rel="stylesheet" href="/ui/datacube/dashboard-override.css">\n'
)
_DASHBOARD_BODY_INJECT = (
    '<script src="/ui/datacube/lightbox.js"></script>\n'
)

@app.get("/api/projects/{project_id}/datacube/files/{file_path:path}")
async def datacube_file_serve(
    project_id: str,
    file_path: str,
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(_security_optional),
):
    """Отдать файл артефактов Data Cube.

    Аутентификация: Basic Auth header, ?_auth=base64(user:pass), или cookie gis_session.
    При первом успехе через header/param выставляет сессионную cookie — это позволяет
    переходить по внутренним ссылкам dashboard-страниц без повторной авторизации.
    HTML-файлы дополнительно получают инъекцию /ui/datacube/dashboard-override.css.
    """
    user, set_cookie = _resolve_auth(request, credentials)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Basic"},
        )

    project_path = Path(PROJECTS_DIR) / project_id
    if not project_path.exists():
        raise HTTPException(status_code=404, detail="Project not found")

    dc_dir = _find_dc_dir(project_path)
    if dc_dir is None:
        raise HTTPException(status_code=404, detail="No Data Cube artifacts found")

    try:
        target = (dc_dir / file_path).resolve()
        if not str(target).startswith(str(dc_dir.resolve())):
            raise HTTPException(status_code=403, detail="Forbidden")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid path")

    if not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # HTML files: inject CSS override + lightbox JS, return as HTMLResponse
    if target.suffix.lower() == ".html":
        html = target.read_text(encoding="utf-8", errors="replace")
        if "</head>" in html:
            html = html.replace("</head>", _DASHBOARD_HEAD_INJECT + "</head>", 1)
        if "</body>" in html:
            html = html.replace("</body>", _DASHBOARD_BODY_INJECT + "</body>", 1)
        else:
            html += _DASHBOARD_BODY_INJECT
        resp: Response = HTMLResponse(html)
    else:
        resp = FileResponse(str(target))

    if set_cookie:
        resp.set_cookie(
            "gis_session", _new_session(),
            httponly=True, samesite="lax", max_age=_SESSION_TTL,
        )
    return resp