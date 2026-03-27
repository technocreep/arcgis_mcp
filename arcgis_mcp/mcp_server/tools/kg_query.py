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
            logger.info("geo_context_query | project_id=%s | effective_query: %s", pid, effective_query)
        else:
            logger.info("geo_context_query | query: %s", query)

        # NL → Cypher
        from rag.nl_to_cypher import nl_query_to_cypher
        cypher, nl_err = nl_query_to_cypher(effective_query)
        if nl_err:
            logger.error("NL→Cypher failed: %s", nl_err)
            return json.dumps({
                "error": nl_err,
                "query": query,
                "hint": "Проверьте настройки KG_LLM_BASE_URL и KG_LLM_MODEL в .env",
            }, ensure_ascii=False)

        # Выполнить Cypher
        try:
            results = kg.execute(cypher)
            logger.info("Neo4j result count: %d", len(results))
        except Exception as e:
            logger.error("Neo4j execution error: %s | cypher: %s", e, cypher)
            return json.dumps({
                "error": f"Ошибка выполнения Cypher: {e}",
                "cypher": cypher,
                "query": query,
            }, ensure_ascii=False)

        payload = _compress_results(results, query=query, cypher=cypher)
        logger.info("Response: total=%d compression=%s", len(results), payload.get("compression_applied", False))
        return json.dumps(payload, ensure_ascii=False, indent=2)

    return [geo_context_query]


# ---------------------------------------------------------------------------
# Response compression
# ---------------------------------------------------------------------------

_CARD_KEYS = {"year_start", "title", "reg_number", "abstract_results"}
_COMPRESSION_THRESHOLD = 50
_SAMPLE_SIZE = 20


def _compress_results(results: list[dict], *, query: str, cypher: str) -> dict:
    total = len(results)
    base = {"query": query, "cypher": cypher, "total_count": total}

    if total <= _COMPRESSION_THRESHOLD:
        base["compression_applied"] = False
        base["results"] = results
        return base

    first_keys = set(results[0].keys()) if results else set()
    is_card = bool(first_keys & _CARD_KEYS)

    stats: dict = {}
    if is_card:
        by_decade: dict[str, int] = {}
        by_mineral: dict[str, int] = {}
        by_org: dict[str, int] = {}

        for r in results:
            # decade
            ys = r.get("year_start")
            if isinstance(ys, (int, float)):
                decade = str(int(ys) // 10 * 10) + "s"
                by_decade[decade] = by_decade.get(decade, 0) + 1

            # minerals — may come as list or single string
            minerals = r.get("minerals") or r.get("m.name") or r.get("mineral")
            if isinstance(minerals, list):
                for m in minerals:
                    if m:
                        by_mineral[str(m)] = by_mineral.get(str(m), 0) + 1
            elif minerals:
                by_mineral[str(minerals)] = by_mineral.get(str(minerals), 0) + 1

            # organization
            org = r.get("organization") or r.get("org") or r.get("conducted_by")
            if org:
                by_org[str(org)] = by_org.get(str(org), 0) + 1

        stats["by_decade"] = dict(sorted(by_decade.items()))
        if by_mineral:
            stats["by_mineral"] = dict(sorted(by_mineral.items(), key=lambda x: -x[1]))
        if by_org:
            top5_org = sorted(by_org.items(), key=lambda x: -x[1])[:5]
            stats["top_organizations"] = dict(top5_org)
    else:
        for key in first_keys:
            vals = [r[key] for r in results if r.get(key) is not None]
            if not vals:
                continue
            if all(isinstance(v, (int, float)) for v in vals):
                stats[key] = {
                    "min": min(vals),
                    "max": max(vals),
                    "mean": round(sum(vals) / len(vals), 4),
                }
            else:
                counts: dict[str, int] = {}
                for v in vals:
                    sv = str(v)
                    counts[sv] = counts.get(sv, 0) + 1
                top5 = sorted(counts.items(), key=lambda x: -x[1])[:5]
                stats[key] = {"unique_count": len(counts), "top_values": dict(top5)}

    base["compression_applied"] = True
    base["compression_reason"] = (
        f"Результатов: {total} (порог {_COMPRESSION_THRESHOLD}). "
        f"Показана статистика + {min(_SAMPLE_SIZE, total)} примеров."
    )
    base["statistics"] = stats
    base["sample"] = results[:_SAMPLE_SIZE]
    base["hint"] = (
        "Для уточнения добавьте фильтры в запрос: год, организация, минерал, номенклатурный лист. "
        "Пример: 'карточки по меди 1970-1985 организация ВСЕГЕИ'."
    )
    return base
