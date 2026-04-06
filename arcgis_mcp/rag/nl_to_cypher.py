"""NL → Cypher конвертер через vLLM (OpenAI-совместимый API).

Принимает natural language запрос, возвращает Cypher-запрос для Neo4j KG.
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

logger = logging.getLogger(__name__)

SCHEMA_CONTEXT = """
You are a Neo4j Cypher expert. Your ONLY task is to convert a natural language question
into a valid Cypher query. You MUST follow every rule below without exception.

=== SCHEMA ===

Nodes:
- Project         {id, name, primary_crs, extent_json}
- Group           {id, name, project_id, feature_dataset}
- Layer           {id, project_id, display_name, geometry_type, feature_count,
                   extent_json, crs_epsg, is_large, group_name, feature_dataset, units}
- Field           {id, layer_id, project_id, name, dtype,
                   min_val, max_val, mean, unique_count, top_values_json}
- Attachment      {id, layer_id, project_id, att_name, content_type}
- InvestigationCard {reg_number, title, authors, organization,
                     year_start, year_end, minerals_json, work_type, scale,
                     area_km2, bbox_json, keywords_json,
                     abstract_results, abstract_conclusions,
                     sheet_nomenclature, region_okrug, region_oblast,
                     purpose, reserves_calculated, completion_status}
- Mineral         {name}
- Organization    {name}
- WorkMethod      {name, work_type, scale}
- DatacubeBlock   {block_id, project_id, score, lon, lat,
                   dominant_driver,        <- raw ML feature name, NOT for semantic search
                   dominant_driver_group}  <- human-readable group (other/hydrothermal/structural/…)
- SpatialTile     {id, layer_id, project_id, bbox_json, feature_count, dominant_values_json}

Relationships (direction is FIXED — never reverse):
  (Project)-[:HAS_LAYER]           ->(Layer)
  (Project)-[:HAS_GROUP]           ->(Group)
  (Project)-[:HAS_BLOCK]           ->(DatacubeBlock)
  (Layer)-[:IN_GROUP]              ->(Group)
  (Layer)-[:HAS_FIELD]             ->(Field)
  (Layer)-[:HAS_ATTACHMENT]        ->(Attachment)
  (Layer)-[:HAS_TILE]              ->(SpatialTile)
  (Attachment)-[:IS_CARD]          ->(InvestigationCard)
  (InvestigationCard)-[:TARGETS]           ->(Mineral)
  (InvestigationCard)-[:CONDUCTED_BY]      ->(Organization)
  (InvestigationCard)-[:USES_METHOD]       ->(WorkMethod)
  (InvestigationCard)-[:SPATIALLY_COVERS]  ->(Layer)

=== CANONICAL PATTERNS ===

# Cards for a project (via spatial coverage):
MATCH (p:Project {id: 'X'})-[:HAS_LAYER]->(l:Layer)
MATCH (c:InvestigationCard)-[:SPATIALLY_COVERS]->(l)

# Cards for a project (via attachments):
MATCH (p:Project {id: 'X'})-[:HAS_LAYER]->(l:Layer)
      -[:HAS_ATTACHMENT]->(a:Attachment)-[:IS_CARD]->(c:InvestigationCard)

# Cards with work method for a project:
MATCH (c:InvestigationCard)-[:SPATIALLY_COVERS]->(l:Layer {project_id: 'X'})
MATCH (c)-[:USES_METHOD]->(wm:WorkMethod)

# Cards by mineral (use for ANY question about mineral content, assays, deposits):
MATCH (c:InvestigationCard)-[:TARGETS]->(m:Mineral)
WHERE toLower(m.name) CONTAINS 'золото'

# Cards by mineral filtered by project:
MATCH (p:Project {id: 'X'})-[:HAS_LAYER]->(l:Layer)
      -[:HAS_ATTACHMENT]->(a:Attachment)-[:IS_CARD]->(c:InvestigationCard)
      -[:TARGETS]->(m:Mineral)
WHERE toLower(m.name) CONTAINS 'мед'
RETURN c.title, c.year_start, c.year_end, c.abstract_results, m.name

# Top prospective blocks for a project:
MATCH (p:Project {id: 'X'})-[:HAS_BLOCK]->(b:DatacubeBlock)
RETURN b.block_id, b.score, b.lon, b.lat, b.dominant_driver_group
ORDER BY b.score DESC LIMIT 50

# Blocks filtered by driver group:
MATCH (p:Project {id: 'X'})-[:HAS_BLOCK]->(b:DatacubeBlock)
WHERE toLower(b.dominant_driver_group) CONTAINS 'hydrothermal'
RETURN b.block_id, b.score, b.lon, b.lat
ORDER BY b.score DESC

=== RULES ===

1. Return ONLY the Cypher query — no markdown fences, no explanation, no comments.
2. NEVER reverse relationship directions. They are fixed as shown in SCHEMA above.
3. SPATIALLY_COVERS: always (InvestigationCard)-[:SPATIALLY_COVERS]->(Layer), never reversed.
4. When project_id is given (e.g. "[project_id filter: X]"), filter with:
   WHERE l.project_id = 'X'  OR  MATCH (p:Project {id: 'X'})-[...]
5. Use DISTINCT when traversing multiple paths to the same node.
6. Mineral and text search: use toLower(...) CONTAINS 'term' (lowercase term).
   Mineral name variants: медь/мед/copper → 'мед'; золото/gold → 'золот'; свинец → 'свинец'; цинк → 'цинк'.
7. minerals_json, keywords_json are JSON strings — use CONTAINS for substring search.
6a. IMPORTANT: "содержание <минерала>" / "<минерал> в скважинах/пробах/породах" —
    this is NOT about Field nodes. Always query InvestigationCard → Mineral for mineral content questions.
    Field nodes only store GDB column names and statistics (min/max/mean), not mineral assay data.
8. Year filter: WHERE c.year_start >= 1960 AND c.year_end <= 1980
9. LIMIT rules:
   - General/exploratory queries (no specific filter): LIMIT 50.
   - Queries filtered by project, area, mineral, year, or organization: NO LIMIT.
   - Count queries: use COUNT(), no LIMIT.
10. Use meaningful aliases in RETURN (e.g. year_start, not c.year_start).
11. For work type / scale queries always JOIN WorkMethod via USES_METHOD.
12. Do NOT invent properties or relationships not listed in SCHEMA.
13. DatacubeBlock driver filtering: ALWAYS use dominant_driver_group (human-readable group).
    NEVER filter on dominant_driver — it is a raw ML feature name, not semantic.
14. DatacubeBlock score: float 0..1, higher = more prospective.
    High-confidence filter: WHERE b.score >= 0.5
"""


def nl_query_to_cypher(query: str) -> tuple[str, str | None]:
    """Конвертировать NL запрос в Cypher.

    Returns:
        (cypher, error) — error=None если успешно.
    """
    logger.info("NL→Cypher | query: %s", query)
    logger.info("Connection try to: %s >>> %s", config.KG_LLM_BASE_URL, config.KG_LLM_MODEL)

    if not config.KG_LLM_BASE_URL or not config.KG_LLM_MODEL:
        return "", "KG_LLM_BASE_URL или KG_LLM_MODEL не настроены"

    try:
        from openai import OpenAI
        client = OpenAI(
            base_url=config.KG_LLM_BASE_URL,
            api_key=config.KG_LLM_API_KEY or "none",
        )
    except ImportError:
        return "", "openai пакет не установлен"

    for attempt in range(2):
        user_msg = query if attempt == 0 else f"{query}\n\nПредыдущий запрос вызвал ошибку синтаксиса. Исправь."
        try:
            response = client.chat.completions.create(
                model=config.KG_LLM_MODEL,
                messages=[
                    {"role": "system", "content": SCHEMA_CONTEXT},
                    {"role": "user", "content": f"Convert to Cypher: {user_msg}"},
                ],
                max_tokens=4096,
                temperature=0,
            )
            msg = response.choices[0].message
            content = msg.content
            reasoning = getattr(msg, "reasoning_content", None) or getattr(msg, "reasoning", None)
            logger.debug(
                "LLM raw | content=%r | reasoning=%r",
                content,
                (reasoning or "")[:200],
            )
            # Reasoning-модели (DeepSeek-R1, QwQ и др.) кладут ответ в reasoning,
            # а content остаётся None. Извлекаем Cypher из reasoning-текста.
            if not content and reasoning:
                logger.info("content=None, extracting Cypher from reasoning field")
                # Ищем последний Cypher-блок в тексте reasoning
                cypher_blocks = re.findall(
                    r"```(?:cypher)?\n?([\s\S]*?)```", reasoning
                )
                if cypher_blocks:
                    content = cypher_blocks[-1].strip()
                    logger.info("Extracted Cypher from reasoning block: %s", content[:200])
                else:
                    # Ищем строки MATCH/RETURN/WITH как признак Cypher
                    lines = reasoning.splitlines()
                    cypher_lines: list[str] = []
                    in_cypher = False
                    for line in lines:
                        stripped = line.strip()
                        if re.match(r"^(MATCH|WITH|WHERE|RETURN|OPTIONAL|CALL|UNWIND|CREATE|MERGE)", stripped, re.I):
                            in_cypher = True
                        if in_cypher:
                            cypher_lines.append(line)
                    if cypher_lines:
                        content = "\n".join(cypher_lines).strip()
                        logger.info("Extracted Cypher from reasoning lines: %s", content[:200])
            if not content:
                raise ValueError(f"LLM returned None/empty content. message={msg}")
            # Некоторые серверы вставляют <think>...</think> внутрь content
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            # Модели-reasoning иногда закрывают блок </think> без открывающего тега
            if "</think>" in content:
                content = content.split("</think>")[-1].strip()
            if not content:
                raise ValueError("LLM ответил только thinking-блоком без Cypher")
            cypher = content
            # Убрать markdown-блоки если модель добавила
            cypher = re.sub(r"```(?:cypher)?\n?", "", cypher).strip("`").strip()
            # Финальный fallback: если результат не начинается с Cypher-ключевого слова,
            # извлечь первый непрерывный блок Cypher-строк
            _cypher_kw = re.compile(
                r"^(MATCH|WITH|WHERE|RETURN|OPTIONAL|CALL|UNWIND|CREATE|MERGE)", re.I
            )
            if cypher and not _cypher_kw.match(cypher.lstrip()):
                _lines = cypher.splitlines()
                _cypher_lines: list[str] = []
                _in_cypher = False
                for _line in _lines:
                    if _cypher_kw.match(_line.strip()):
                        _in_cypher = True
                    if _in_cypher:
                        _cypher_lines.append(_line)
                if _cypher_lines:
                    cypher = "\n".join(_cypher_lines).strip()
                    logger.info("Fallback: extracted Cypher block from mixed content")
            logger.info("Generated Cypher:\n%s", cypher)
            return cypher, None
        except Exception as e:
            logger.warning("LLM attempt %d failed: %s", attempt + 1, e)
            if attempt == 1:
                return "", f"LLM ошибка: {e}"

    return "", "Не удалось получить Cypher"
