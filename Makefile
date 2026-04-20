# =============================================================================
# SNIES Data Platform - Makefile
# =============================================================================
# Uso:
#   make help        -> lista comandos disponibles
#   make setup       -> primer arranque (copia .env, crea carpetas)
#   make up          -> levanta toda la plataforma
#   make down        -> apaga (conserva datos)
#   make clean       -> apaga y borra TODOS los datos
#   make logs        -> tail de logs de todos los servicios
#   make ps          -> estado de los contenedores
#   make shell-airflow -> entra al contenedor de Airflow
#   make shell-dwh   -> entra a psql del DWH
#   make dbt-run     -> ejecuta 'dbt run' dentro del contenedor
#   make dbt-test    -> ejecuta 'dbt test' dentro del contenedor
#   make dbt-docs    -> genera y sirve la documentación de dbt
# =============================================================================

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Colores para la salida
CYAN   := \033[36m
YELLOW := \033[33m
GREEN  := \033[32m
RESET  := \033[0m

# -----------------------------------------------------------------------------
# Ayuda
# -----------------------------------------------------------------------------
.PHONY: help
help: ## Muestra esta ayuda
	@echo ""
	@echo "$(CYAN)SNIES Data Platform — comandos disponibles$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-18s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# -----------------------------------------------------------------------------
# Setup inicial
# -----------------------------------------------------------------------------
.PHONY: setup
setup: ## Primer arranque: copia .env.example a .env si no existe
	@if [ ! -f .env ]; then \
		cp .env.example .env && \
		echo "$(GREEN)✓ Creado .env desde .env.example$(RESET)"; \
		echo "$(YELLOW)  → Revisa y ajusta las credenciales antes de 'make up'$(RESET)"; \
	else \
		echo "$(YELLOW)✓ .env ya existe, no se sobrescribe$(RESET)"; \
	fi
	@mkdir -p airflow/logs airflow/plugins airflow/dags/utils
	@mkdir -p dbt/models/bronze dbt/models/silver dbt/models/gold dbt/seeds dbt/tests
	@echo "$(GREEN)✓ Estructura de carpetas lista$(RESET)"

# -----------------------------------------------------------------------------
# Ciclo de vida de la plataforma
# -----------------------------------------------------------------------------
.PHONY: build
build: ## Construye las imágenes Docker custom
	docker compose build airflow-scheduler

.PHONY: up
up: ## Levanta toda la plataforma en background
	docker compose up -d
	@echo ""
	@echo "$(GREEN)✓ Plataforma levantada$(RESET)"
	@echo ""
	@echo "  Airflow UI:  http://localhost:8080"
	@echo "  Metabase:    http://localhost:3000"
	@echo "  DWH (psql):  localhost:5432"
	@echo ""

.PHONY: down
down: ## Apaga los contenedores (conserva volúmenes y datos)
	docker compose down

.PHONY: restart
restart: down up ## Reinicia la plataforma

.PHONY: clean
clean: ## ⚠️  Apaga TODO y borra volúmenes (pierdes los datos)
	@echo "$(YELLOW)⚠️  Esto borra todos los datos (DWH + metadata Airflow).$(RESET)"
	@read -p "¿Continuar? [y/N] " ans && [ "$$ans" = "y" ] || exit 1
	docker compose down -v
	@echo "$(GREEN)✓ Limpieza completa$(RESET)"

# -----------------------------------------------------------------------------
# Observabilidad
# -----------------------------------------------------------------------------
.PHONY: ps
ps: ## Estado de los contenedores
	docker compose ps

.PHONY: logs
logs: ## Tail de logs (todos los servicios)
	docker compose logs -f --tail=100

.PHONY: logs-airflow
logs-airflow: ## Logs solo del scheduler de Airflow
	docker compose logs -f --tail=100 airflow-scheduler

# -----------------------------------------------------------------------------
# Shells útiles
# -----------------------------------------------------------------------------
.PHONY: shell-airflow
shell-airflow: ## Bash dentro del contenedor de Airflow
	docker compose exec airflow-scheduler bash

.PHONY: shell-dwh
shell-dwh: ## psql contra el Data Warehouse
	docker compose exec dwh-postgres \
		psql -U $$(grep ^DWH_POSTGRES_USER .env | cut -d= -f2) \
		     -d $$(grep ^DWH_POSTGRES_DB   .env | cut -d= -f2)

# -----------------------------------------------------------------------------
# dbt (se ejecuta dentro del contenedor Airflow que ya tiene dbt instalado)
# -----------------------------------------------------------------------------
.PHONY: dbt-deps
dbt-deps: ## Instala dependencias del proyecto dbt
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt deps"

.PHONY: dbt-seed
dbt-seed: ## Carga seeds de dbt (ies_sue, normalización)
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt seed"

.PHONY: dbt-run
dbt-run: ## Ejecuta todos los modelos dbt (Silver + Gold)
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt run"

.PHONY: dbt-test
dbt-test: ## Ejecuta los tests de calidad
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt test"

.PHONY: dbt-docs
dbt-docs: ## Genera y sirve la documentación de dbt en :8081
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8081 --host 0.0.0.0"
