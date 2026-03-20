"""Neo4j клиент — подключение, инициализация схемы, выполнение запросов."""

from __future__ import annotations

import logging
from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

from . import kg_schema

logger = logging.getLogger(__name__)


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._init_schema()

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict]:
        """Выполнить Cypher-запрос, вернуть список словарей."""
        with self._driver.session() as session:
            result = session.run(cypher, params or {})
            return [dict(r) for r in result]

    def merge_node(self, label: str, match_props: dict, set_props: dict | None = None):
        """MERGE узла по ключевым полям, SET дополнительных свойств."""
        match_str = ", ".join(f"{k}: ${k}" for k in match_props)
        cypher = f"MERGE (n:{label} {{{match_str}}})"
        if set_props:
            cypher += " SET n += $set_props"
        params: dict = {**match_props}
        if set_props:
            params["set_props"] = set_props
        self.execute(cypher, params)

    def merge_rel(
        self,
        from_label: str, from_key: str, from_val: Any,
        rel_type: str,
        to_label: str, to_key: str, to_val: Any,
    ):
        """MERGE ребра между двумя существующими узлами."""
        cypher = (
            f"MATCH (a:{from_label} {{{from_key}: $from_val}}), "
            f"(b:{to_label} {{{to_key}: $to_val}}) "
            f"MERGE (a)-[:{rel_type}]->(b)"
        )
        self.execute(cypher, {"from_val": from_val, "to_val": to_val})

    def close(self):
        self._driver.close()

    def _init_schema(self):
        with self._driver.session() as session:
            for stmt in kg_schema.CONSTRAINTS:
                try:
                    session.run(stmt)
                except ClientError as e:
                    logger.debug("Constraint: %s", e)
            for stmt in kg_schema.INDEXES:
                try:
                    session.run(stmt)
                except ClientError as e:
                    logger.debug("Index: %s", e)
            for name, stmt in kg_schema.FULLTEXT_INDEXES:
                try:
                    session.run(stmt)
                except ClientError as e:
                    logger.debug("Fulltext %s: %s", name, e)
        logger.info("Neo4j schema initialized")
