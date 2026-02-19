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
