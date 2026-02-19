"""Абстракция доступа к хранилищу проектов.

Читает manifest.json, layer_profiles/*.json, _index.json.
Предоставляет resolve_layer_name() для нечёткого поиска слоя по имени.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ProjectSummary:
    id: str
    name: str
    created_at: str
    layers_count: int
    has_attachments: bool
    gdb_file: str | None


# ---------------------------------------------------------------------------
# ProjectStore
# ---------------------------------------------------------------------------

class ProjectStore:
    """Чтение данных проектов из файловой системы."""

    def __init__(self, projects_dir: str | Path):
        self.projects_dir = Path(projects_dir)
        self._index_path = self.projects_dir / "_index.json"

    # -----------------------------------------------------------------------
    # Проекты
    # -----------------------------------------------------------------------

    def list_projects(self) -> list[ProjectSummary]:
        """Список всех проектов из _index.json."""
        if not self._index_path.exists():
            return []
        data = self._read_json(self._index_path)
        result = []
        for p in data.get("projects", []):
            result.append(ProjectSummary(
                id=p.get("id", ""),
                name=p.get("name", ""),
                created_at=p.get("created_at", ""),
                layers_count=p.get("layers_count", 0),
                has_attachments=p.get("has_attachments", False),
                gdb_file=p.get("gdb_file"),
            ))
        return result

    def get_manifest(self, project_id: str) -> dict:
        """Прочитать manifest.json проекта."""
        path = self._project_dir(project_id) / "manifest.json"
        if not path.exists():
            raise FileNotFoundError(f"Проект '{project_id}' не найден")
        return self._read_json(path)

    def get_layer_profile(self, project_id: str, layer_id: str) -> dict | None:
        """Прочитать детальный профиль слоя из layer_profiles/."""
        safe = layer_id.replace("/", "_").replace("\\", "_")
        path = self._project_dir(project_id) / "layer_profiles" / f"{safe}.json"
        if not path.exists():
            return None
        return self._read_json(path)

    def get_gdb_path(self, project_id: str) -> str:
        """Путь к .gdb директории проекта."""
        project_dir = self._project_dir(project_id)
        manifest = self.get_manifest(project_id)
        gdb_name = manifest.get("project", {}).get("source_files", {}).get("gdb")
        if gdb_name:
            candidate = project_dir / "data" / gdb_name
            if candidate.exists():
                return str(candidate)
        # Fallback: ищем любую .gdb в data/
        data_dir = project_dir / "data"
        if data_dir.exists():
            for entry in data_dir.iterdir():
                if entry.suffix.lower() == ".gdb" or (
                    entry.is_dir() and entry.name.lower().endswith(".gdb")
                ):
                    return str(entry)
        raise FileNotFoundError(f"Не найдена .gdb для проекта '{project_id}'")

    # -----------------------------------------------------------------------
    # Резолвинг имён слоёв
    # -----------------------------------------------------------------------

    def resolve_layer_name(self, project_id: str, user_query: str) -> str | None:
        """Найти layer_id по display_name, dataset_name или alias.

        Порядок поиска:
            1. Точное совпадение dataset_name (case-insensitive)
            2. Точное совпадение display_name (case-insensitive)
            3. Полное совпадение с alias
            4. Частичное совпадение display_name (токены)
            5. Частичное совпадение alias (токены)

        Returns:
            dataset_name или None
        """
        manifest = self.get_manifest(project_id)
        layers = manifest.get("layers", [])
        aliases_map = manifest.get("aliases", {})
        query_lower = user_query.strip().lower()

        # 1. Точный dataset_name
        for layer in layers:
            if layer["layer_id"].lower() == query_lower:
                return layer["layer_id"]

        # 2. Точный display_name
        for layer in layers:
            if layer.get("display_name", "").lower() == query_lower:
                return layer["layer_id"]

        # 3. Полный alias
        for layer in layers:
            ds = layer["layer_id"]
            for alias in aliases_map.get(ds, []):
                if alias.lower() == query_lower:
                    return ds

        # 4. Частичный display_name (все токены запроса содержатся в display_name)
        query_tokens = set(_tokenize(query_lower))
        for layer in layers:
            dn_tokens = set(_tokenize(layer.get("display_name", "").lower()))
            if query_tokens and query_tokens.issubset(dn_tokens):
                return layer["layer_id"]

        # 5. Частичный alias
        for layer in layers:
            ds = layer["layer_id"]
            for alias in aliases_map.get(ds, []):
                alias_tokens = set(_tokenize(alias.lower()))
                if query_tokens and query_tokens.issubset(alias_tokens):
                    return ds

        return None

    def get_layer_entry(self, manifest: dict, layer_id: str) -> dict | None:
        """Найти запись слоя в manifest по layer_id."""
        for layer in manifest.get("layers", []):
            if layer["layer_id"] == layer_id:
                return layer
        return None

    # -----------------------------------------------------------------------
    # Утилиты
    # -----------------------------------------------------------------------

    def _project_dir(self, project_id: str) -> Path:
        return self.projects_dir / project_id

    @staticmethod
    def _read_json(path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))


def _tokenize(text: str) -> list[str]:
    """Разбить строку на значимые токены (слова длиннее 1 символа)."""
    return [t for t in re.split(r"[\s\-_,.()/]+", text) if len(t) > 1]
