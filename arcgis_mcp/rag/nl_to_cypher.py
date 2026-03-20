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
You are an expert in Neo4j Cypher. Convert natural language questions to Cypher queries.

Knowledge Graph schema for geological GIS data:

Node Labels and properties:
- Project {id: STRING, name: STRING, primary_crs: STRING, extent_json: STRING}
- Layer {id: STRING, project_id: STRING, display_name: STRING, geometry_type: STRING,
         feature_count: INTEGER, extent_json: STRING, crs_epsg: INTEGER,
         is_large: BOOLEAN, group_name: STRING, feature_dataset: STRING, units: STRING}
- Field {id: STRING, layer_id: STRING, project_id: STRING, name: STRING, dtype: STRING,
         min_val: FLOAT, max_val: FLOAT, mean: FLOAT, unique_count: INTEGER, top_values_json: STRING}
- Group {id: STRING, name: STRING, project_id: STRING, feature_dataset: STRING}
- Attachment {id: STRING, layer_id: STRING, project_id: STRING, att_name: STRING, content_type: STRING}
- InvestigationCard {reg_number: STRING, title: STRING, authors: STRING, organization: STRING,
                     year_start: INTEGER, year_end: INTEGER, minerals_json: STRING,
                     work_type: STRING, scale: STRING, area_km2: FLOAT, bbox_json: STRING,
                     keywords_json: STRING, abstract_results: STRING, abstract_conclusions: STRING,
                     sheet_nomenclature: STRING, region_okrug: STRING, region_oblast: STRING,
                     purpose: STRING, reserves_calculated: BOOLEAN, completion_status: STRING}
- Mineral {name: STRING}
- Organization {name: STRING}
- WorkMethod {name: STRING, work_type: STRING, scale: STRING}
- SpatialTile {id: STRING, layer_id: STRING, project_id: STRING, bbox_json: STRING,
               feature_count: INTEGER, dominant_values_json: STRING}
- DatacubeBlock {block_id: STRING, project_id: STRING, score: FLOAT, lon: FLOAT, lat: FLOAT,
                 dominant_driver: STRING, dominant_driver_group: STRING}

Relationships:
(:Project)-[:HAS_LAYER]->(:Layer)
(:Project)-[:HAS_GROUP]->(:Group)
(:Project)-[:HAS_BLOCK]->(:DatacubeBlock)
(:Layer)-[:HAS_FIELD]->(:Field)
(:Layer)-[:IN_GROUP]->(:Group)
(:Layer)-[:HAS_ATTACHMENT]->(:Attachment)
(:Layer)-[:HAS_TILE]->(:SpatialTile)
(:Attachment)-[:IS_CARD]->(:InvestigationCard)
(:InvestigationCard)-[:TARGETS]->(:Mineral)
(:InvestigationCard)-[:CONDUCTED_BY]->(:Organization)
(:InvestigationCard)-[:USES_METHOD]->(:WorkMethod)
(:InvestigationCard)-[:SPATIALLY_COVERS]->(:Layer)

Rules:
1. Return ONLY the Cypher query, no markdown, no explanation.
2. Always use LIMIT 50 unless user asks for count.
3. For mineral search use: WHERE m.name CONTAINS 'gold' (lowercase).
4. For text search in cards use: WHERE toLower(c.title) CONTAINS 'query' OR c.keywords_json CONTAINS 'query'.
5. minerals_json, keywords_json are JSON strings, use CONTAINS for search.
6. For year range: WHERE c.year_start >= 1960 AND c.year_end <= 1980.
7. Always use RETURN with meaningful field aliases.
"""


def nl_query_to_cypher(query: str) -> tuple[str, str | None]:
    """Конвертировать NL запрос в Cypher.

    Returns:
        (cypher, error) — error=None если успешно.
    """
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
                max_tokens=512,
                temperature=0,
            )
            cypher = response.choices[0].message.content.strip()
            # Убрать markdown-блоки если модель добавила
            cypher = re.sub(r"```(?:cypher)?\n?", "", cypher).strip("`").strip()
            return cypher, None
        except Exception as e:
            logger.warning("LLM attempt %d failed: %s", attempt + 1, e)
            if attempt == 1:
                return "", f"LLM ошибка: {e}"

    return "", "Не удалось получить Cypher"
