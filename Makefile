.DEFAULT_GOAL := help
COMPOSE := docker compose
SERVICES := gis-loader gis-mcp data-cube

# ─── Help ─────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  arcgis-mcp — управление контейнерами"
	@echo ""
	@echo "  Запуск / остановка"
	@echo "    make up              — поднять все сервисы (без пересборки)"
	@echo "    make down            — остановить и удалить контейнеры"
	@echo "    make stop            — приостановить контейнеры (без удаления)"
	@echo "    make restart         — перезапустить все сервисы (без пересборки)"
	@echo ""
	@echo "  Пересборка и перезапуск"
	@echo "    make build           — пересобрать gis-loader + gis-mcp (зависимости)"
	@echo "    make rebuild         — пересобрать всё + поднять"
	@echo "    make rebuild-cube    — пересобрать data-cube (клон репо заново)"
	@echo "    make rebuild-app     — пересобрать gis-loader + gis-mcp + поднять"
	@echo ""
	@echo "  Горячая перезагрузка (код примонтирован через volume)"
	@echo "    make reload-loader   — перезапустить gis-loader без пересборки"
	@echo "    make reload-mcp      — перезапустить gis-mcp без пересборки"
	@echo "    make reload-cube     — скопировать server.py в контейнер и перезапустить data-cube"
	@echo ""
	@echo "  Логи"
	@echo "    make logs            — логи всех сервисов (follow)"
	@echo "    make logs-loader     — логи gis-loader"
	@echo "    make logs-mcp        — логи gis-mcp"
	@echo "    make logs-cube       — логи data-cube"
	@echo ""
	@echo "  Диагностика"
	@echo "    make ps              — статус контейнеров"
	@echo "    make sh-loader       — shell в gis-loader"
	@echo "    make sh-mcp          — shell в gis-mcp"
	@echo "    make sh-cube         — shell в data-cube"
	@echo ""
	@echo "  Очистка"
	@echo "    make clean           — down + удалить volumes (осторожно: данные проектов)"
	@echo "    make prune           — удалить неиспользуемые образы"
	@echo ""

# ─── Запуск / остановка ───────────────────────────────────────────────────────

# Поднять все сервисы без пересборки (обычный старт)
.PHONY: up
up:
	$(COMPOSE) up -d

# Остановить и удалить контейнеры (volumes сохраняются)
.PHONY: down
down:
	$(COMPOSE) down

# Приостановить без удаления
.PHONY: stop
stop:
	$(COMPOSE) stop

# Перезапустить все без пересборки (применяется при правках конфига/env)
.PHONY: restart
restart:
	$(COMPOSE) restart

# ─── Пересборка ───────────────────────────────────────────────────────────────

# Пересобрать образы gis-loader и gis-mcp (используют общий Dockerfile)
# Нужно при: изменении requirements, Dockerfile, системных зависимостей
.PHONY: build
build:
	$(COMPOSE) build gis-loader gis-mcp

# Пересобрать всё и поднять
.PHONY: rebuild
rebuild:
	$(COMPOSE) build
	$(COMPOSE) up -d

# Пересобрать только gis-loader + gis-mcp и перезапустить их
# Нужно при: изменении requirements.txt, Dockerfile (не затрагивая data-cube)
.PHONY: rebuild-app
rebuild-app:
	$(COMPOSE) build gis-loader gis-mcp
	$(COMPOSE) up -d --no-deps gis-loader gis-mcp

# Пересобрать data-cube (клонирует репо заново через GITHUB_TOKEN)
# Нужно при: обновлении Data_cube репозитория
# CACHE_BUST сбрасывает кеш слоя git clone
.PHONY: rebuild-cube
rebuild-cube:
	CACHE_BUST=$$(date +%s) $(COMPOSE) build --no-cache data-cube
	$(COMPOSE) up -d --no-deps data-cube

# ─── Горячая перезагрузка (код примонтирован, пересборка не нужна) ────────────

# Применяется при: правках кода в arcgis_mcp/ (uvicorn не в режиме --reload)
.PHONY: reload-loader
reload-loader:
	$(COMPOSE) restart gis-loader

.PHONY: reload-mcp
reload-mcp:
	$(COMPOSE) restart gis-mcp

# Скопировать локальный server.py в контейнер и перезапустить
# Нужно при: правках Data_cube/api/server.py (код не монтируется, клонируется из git)
.PHONY: reload-cube
reload-cube:
	docker cp Data_cube/api/server.py data-cube-server:/app/data_cube/api/server.py
	docker restart data-cube-server

# ─── Логи ─────────────────────────────────────────────────────────────────────

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: logs-loader
logs-loader:
	$(COMPOSE) logs -f gis-loader

.PHONY: logs-mcp
logs-mcp:
	$(COMPOSE) logs -f gis-mcp

.PHONY: logs-cube
logs-cube:
	$(COMPOSE) logs -f data-cube

# ─── Диагностика ──────────────────────────────────────────────────────────────

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: sh-loader
sh-loader:
	$(COMPOSE) exec gis-loader /bin/bash

.PHONY: sh-mcp
sh-mcp:
	$(COMPOSE) exec gis-mcp /bin/bash

.PHONY: sh-cube
sh-cube:
	$(COMPOSE) exec data-cube /bin/bash

# ─── Очистка ──────────────────────────────────────────────────────────────────

# Удалить контейнеры И volumes (projects том — осторожно, данные проектов потеряются)
.PHONY: clean
clean:
	$(COMPOSE) down -v

# Удалить неиспользуемые образы (освобождает место на диске)
.PHONY: prune
prune:
	docker image prune -f
