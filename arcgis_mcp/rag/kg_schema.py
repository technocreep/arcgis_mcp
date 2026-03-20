"""Cypher DDL statements для инициализации схемы Neo4j KG."""

CONSTRAINTS = [
    "CREATE CONSTRAINT project_id IF NOT EXISTS FOR (p:Project) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT layer_id IF NOT EXISTS FOR (l:Layer) REQUIRE l.id IS UNIQUE",
    "CREATE CONSTRAINT field_id IF NOT EXISTS FOR (f:Field) REQUIRE f.id IS UNIQUE",
    "CREATE CONSTRAINT attachment_id IF NOT EXISTS FOR (a:Attachment) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT tile_id IF NOT EXISTS FOR (t:SpatialTile) REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT card_reg IF NOT EXISTS FOR (c:InvestigationCard) REQUIRE c.reg_number IS UNIQUE",
    "CREATE CONSTRAINT mineral_name IF NOT EXISTS FOR (m:Mineral) REQUIRE m.name IS UNIQUE",
    "CREATE CONSTRAINT org_name IF NOT EXISTS FOR (o:Organization) REQUIRE o.name IS UNIQUE",
    "CREATE CONSTRAINT block_id IF NOT EXISTS FOR (b:DatacubeBlock) REQUIRE b.block_id IS UNIQUE",
]

INDEXES = [
    "CREATE INDEX layer_project IF NOT EXISTS FOR (l:Layer) ON (l.project_id)",
    "CREATE INDEX field_layer IF NOT EXISTS FOR (f:Field) ON (f.layer_id)",
    "CREATE INDEX attachment_layer IF NOT EXISTS FOR (a:Attachment) ON (a.layer_id)",
    "CREATE INDEX card_year IF NOT EXISTS FOR (c:InvestigationCard) ON (c.year_start, c.year_end)",
    "CREATE INDEX block_project IF NOT EXISTS FOR (b:DatacubeBlock) ON (b.project_id)",
]

FULLTEXT_INDEXES = [
    (
        "card_fulltext",
        "CREATE FULLTEXT INDEX card_fulltext IF NOT EXISTS "
        "FOR (c:InvestigationCard) ON EACH "
        "[c.title, c.keywords_json, c.abstract_results, c.purpose, c.abstract_conclusions]",
    ),
]
