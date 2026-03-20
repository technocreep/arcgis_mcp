"""MCP инструмент geo_context_query — запросы к Knowledge Graph.

Принимает natural language запрос, конвертирует через LLM в Cypher,
выполняет на Neo4j и возвращает результат агенту.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import config

logger = logging.getLogger(__name__)


def make_tools(state: dict) -> list[Callable]:

    def _get_kg():
        """Lazy-инициализация Neo4j клиента."""
        if not getattr(_get_kg, "_client", None):
            if not config.NEO4J_URI:
                return None, "NEO4J_URI не настроен"
            try:
                from rag.kg_client import Neo4jClient
                _get_kg._client = Neo4jClient(
                    config.NEO4J_URI,
                    config.NEO4J_USER,
                    config.NEO4J_PASSWORD,
                )
            except Exception as e:
                return None, f"Ошибка подключения к Neo4j: {e}"
        return _get_kg._client, None

    def geo_context_query(
        query: str,
        project_id: str | None = None,
    ) -> str:
        """Query the geological knowledge graph using natural language.

        Use this tool for questions about:
        - Investigation cards (карточки изученности) by mineral, area, organization, period
        - What geological studies have been conducted in a region
        - Spatial coverage: which layers or cards cover a given area
        - Relationships between layers, groups, fields
        - Data Cube prospectivity blocks and dominant drivers

        Examples:
            "карточки изученности по меди в листе R-42"
            "работы 1960-1980 в Тюменской области"
            "какие слои связаны с аномалиями золота"
            "организации проводившие геологическую съёмку"
            "блоки с высоким скором перспективности"

        Args:
            query: Вопрос на естественном языке (русский или английский).
            project_id: Опционально — ограничить поиск конкретным проектом.
        """
        kg, err = _get_kg()
        if err:
            return json.dumps({"error": err}, ensure_ascii=False)

        # Добавить фильтр по проекту если задан
        effective_query = query
        pid = project_id or state.get("current_project_id")
        if pid:
            effective_query = f"{query} [project_id filter: {pid}]"

        # NL → Cypher
        from rag.nl_to_cypher import nl_query_to_cypher
        cypher, nl_err = nl_query_to_cypher(effective_query)
        if nl_err:
            return json.dumps({
                "error": nl_err,
                "query": query,
                "hint": "Проверьте настройки KG_LLM_BASE_URL и KG_LLM_MODEL в .env",
            }, ensure_ascii=False)

        # Выполнить Cypher
        try:
            results = kg.execute(cypher)
        except Exception as e:
            return json.dumps({
                "error": f"Ошибка выполнения Cypher: {e}",
                "cypher": cypher,
                "query": query,
            }, ensure_ascii=False)

        return json.dumps({
            "query": query,
            "cypher": cypher,
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)

    return [geo_context_query]
