"""Конфигурация GIS Agent Service."""

import os
from pathlib import Path

# Корень проекта
BASE_DIR = Path(__file__).parent

# Хранилище проектов
# В Docker передаётся через PROJECTS_DIR=/app/projects, локально — рядом с кодом
PROJECTS_DIR = Path(os.getenv("PROJECTS_DIR", str(BASE_DIR / "projects")))
PROJECTS_DIR.mkdir(parents=True, exist_ok=True)

# Индекс проектов
INDEX_FILE = PROJECTS_DIR / "_index.json"

# Аутентификация Ingestion API
AUTH_USERNAME = os.getenv("GIS_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("GIS_PASSWORD", "secret")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24

# Лимиты
GDB_LARGE_LAYER_THRESHOLD = 10_000   # слои > N объектов — только schema+count+extent
GDB_STATS_TOP_VALUES_LIMIT = 20      # топ-N категориальных значений

# Версия pipeline
PIPELINE_VERSION = "0.1"
MANIFEST_VERSION = "1.0"

# MinIO (объектное хранилище для выходных файлов визуализации)
# MINIO_ENDPOINT     — адрес S3 API (внутри Docker)
# MINIO_PUBLIC_HOST  — адрес, доступный снаружи (для URL в ответах инструментов)
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "212.41.21.72:9000")
MINIO_PUBLIC_HOST = os.getenv("MINIO_PUBLIC_HOST", "212.41.21.72:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY",  "minio123")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET",      "gis-viz")
