.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help
help:
	@echo ""
	@echo "  make update-cube   — git pull + pip install в контейнере + перезапуск стека"
	@echo "  make rebuild-cube  — пересборка образа data-cube с нуля (новый Dockerfile / apt-пакеты)"
	@echo "  make reload-app    — перезапустить gis-loader + gis-mcp без пересборки (правки кода)"
	@echo "  make rebuild-app   — пересобрать gis-loader + gis-mcp и перезапустить"
	@echo "  make rebuild       — пересобрать всё и поднять (без удаления данных)"
	@echo ""

# ─── Data Cube ────────────────────────────────────────────────────────────────

# Новый коммит в Data_cube → применить без пересборки образа
.PHONY: update-cube
update-cube:
	git -C Data_cube pull
	$(COMPOSE) restart data-cube

# Изменился Dockerfile.datacube или нужны новые системные пакеты
.PHONY: rebuild-cube
rebuild-cube:
	CACHE_BUST=$$(date +%s) $(COMPOSE) build --no-cache data-cube
	$(COMPOSE) up -d --no-deps data-cube

# ─── Фронт (gis-loader + gis-mcp) ────────────────────────────────────────────

# Правки кода в arcgis_mcp/ (код примонтирован через volume)
.PHONY: reload-app
reload-app:
	$(COMPOSE) restart gis-loader gis-mcp

# Изменились зависимости или Dockerfile
.PHONY: rebuild-app
rebuild-app:
	$(COMPOSE) build gis-loader gis-mcp
	$(COMPOSE) up -d --no-deps gis-loader gis-mcp

# ─── Полная пересборка ────────────────────────────────────────────────────────

# Пересобрать всё и поднять (volumes с данными проектов сохраняются)
.PHONY: rebuild
rebuild:
	CACHE_BUST=$$(date +%s) $(COMPOSE) build --no-cache
	$(COMPOSE) down
	$(COMPOSE) up -d
