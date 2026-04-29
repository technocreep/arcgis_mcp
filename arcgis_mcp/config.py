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
AUTH_USERNAME = os.getenv("GIS_USERNAME", "")
AUTH_PASSWORD = os.getenv("GIS_PASSWORD", "")
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24

# Лимиты
GDB_LARGE_LAYER_THRESHOLD = 10_000   # слои > N объектов — только schema+count+extent
GDB_STATS_TOP_VALUES_LIMIT = 20      # топ-N категориальных значений

# Версия pipeline
PIPELINE_VERSION = "0.1"
MANIFEST_VERSION = "1.0"

# Neo4j Knowledge Graph
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "gis_password")

# LLM для NL→Cypher (vLLM, OpenAI-совместимый API)
KG_LLM_BASE_URL = os.getenv("KG_LLM_BASE_URL", "http://host.docker.internal:8000/v1")
KG_LLM_MODEL    = os.getenv("KG_LLM_MODEL",    "")
KG_LLM_API_KEY  = os.getenv("KG_LLM_API_KEY",  "none")

# PDF-парсер карточек изученности
# PDF_PARSER_BACKEND: "local" — локальная vLLM-модель (KG_LLM_*),
#                     "openrouter" — anthropic/claude-haiku-4-5 через OpenRouter
PDF_PARSER_BACKEND  = os.getenv("PDF_PARSER_BACKEND",  "local")
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY",  "")
# PDF_PARSE_WORKERS: 0 = авто (4 для local, 10 для openrouter)
PDF_PARSE_WORKERS   = int(os.getenv("PDF_PARSE_WORKERS", "0"))

# MinIO (объектное хранилище для выходных файлов визуализации)
# MINIO_ENDPOINT     — адрес S3 API (внутри Docker)
# MINIO_PUBLIC_HOST  — адрес, доступный снаружи (для URL в ответах инструментов)
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "")
MINIO_PUBLIC_HOST = os.getenv("MINIO_PUBLIC_HOST", "")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY",  "")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY",  "")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET",      "")
MINIO_CUBE_BUCKET = os.getenv("MINIO_CUBE_BUCKET", "")
