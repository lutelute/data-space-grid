# Makefile
#
# Development commands for the Federated Data Space prototype.
#
# Targets:
#   setup          - Create virtual environment and install all dependencies
#   run-catalog    - Start the Federated Catalog service (port 8000)
#   run-dso        - Start the DSO participant node (port 8001)
#   run-aggregator - Start the Aggregator participant node (port 8002)
#   run-prosumer   - Start the Prosumer participant node (port 8003)
#   run-all        - Start all nodes (catalog + participants) in background
#   test           - Run the full test suite (pytest)
#   test-unit      - Run unit tests only
#   test-integration - Run integration tests only
#   lint           - Run linter (ruff check)
#   format         - Run formatter (ruff format)
#   certs          - Generate development mTLS certificates
#   docker-up      - Start all services via Docker Compose
#   docker-down    - Stop all Docker Compose services
#   clean          - Remove build artifacts, caches, and generated files

.PHONY: setup run-catalog run-dso run-aggregator run-prosumer run-all \
        test test-unit test-integration lint format certs \
        docker-up docker-down clean help

.DEFAULT_GOAL := help

# ── Configuration ─────────────────────────────────────────────────────────

PYTHON     := python3
VENV       := .venv
VENV_BIN   := $(VENV)/bin
PIP        := $(VENV_BIN)/pip
PYTEST     := $(VENV_BIN)/pytest
RUFF       := $(VENV_BIN)/ruff
UVICORN    := $(VENV_BIN)/uvicorn

CERTS_DIR  := infrastructure/certs

# Participant SSL arguments (for local development)
DSO_SSL        := --ssl-keyfile $(CERTS_DIR)/dso.key --ssl-certfile $(CERTS_DIR)/dso.crt --ssl-ca-certs $(CERTS_DIR)/ca.crt
AGGREGATOR_SSL := --ssl-keyfile $(CERTS_DIR)/aggregator.key --ssl-certfile $(CERTS_DIR)/aggregator.crt --ssl-ca-certs $(CERTS_DIR)/ca.crt
PROSUMER_SSL   := --ssl-keyfile $(CERTS_DIR)/prosumer.key --ssl-certfile $(CERTS_DIR)/prosumer.crt --ssl-ca-certs $(CERTS_DIR)/ca.crt

# ── Setup ─────────────────────────────────────────────────────────────────

setup: ## Create virtual environment and install all dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -e ".[dev]"
	@echo ""
	@echo "Setup complete. Activate with: source $(VENV)/bin/activate"

# ── Run Services ──────────────────────────────────────────────────────────

run-catalog: ## Start the Federated Catalog service (port 8000)
	$(UVICORN) src.catalog.main:app --host 0.0.0.0 --port 8000 --reload

run-dso: ## Start the DSO participant node (port 8001)
	$(UVICORN) src.participants.dso.main:app --host 0.0.0.0 --port 8001 $(DSO_SSL) --reload

run-aggregator: ## Start the Aggregator participant node (port 8002)
	$(UVICORN) src.participants.aggregator.main:app --host 0.0.0.0 --port 8002 $(AGGREGATOR_SSL) --reload

run-prosumer: ## Start the Prosumer participant node (port 8003)
	$(UVICORN) src.participants.prosumer.main:app --host 0.0.0.0 --port 8003 $(PROSUMER_SSL) --reload

run-all: ## Start all nodes (catalog + participants) in background
	@echo "Starting all Federated Data Space nodes..."
	$(UVICORN) src.catalog.main:app --host 0.0.0.0 --port 8000 &
	$(UVICORN) src.participants.dso.main:app --host 0.0.0.0 --port 8001 $(DSO_SSL) &
	$(UVICORN) src.participants.aggregator.main:app --host 0.0.0.0 --port 8002 $(AGGREGATOR_SSL) &
	$(UVICORN) src.participants.prosumer.main:app --host 0.0.0.0 --port 8003 $(PROSUMER_SSL) &
	@echo ""
	@echo "All nodes started in background."
	@echo "  Catalog:    http://localhost:8000"
	@echo "  DSO:        https://localhost:8001"
	@echo "  Aggregator: https://localhost:8002"
	@echo "  Prosumer:   https://localhost:8003"
	@echo ""
	@echo "Stop with: kill %1 %2 %3 %4"

# ── Testing ───────────────────────────────────────────────────────────────

test: ## Run the full test suite
	$(PYTEST) tests/ -v --tb=short

test-unit: ## Run unit tests only
	$(PYTEST) tests/unit/ -v --tb=short

test-integration: ## Run integration tests only
	$(PYTEST) tests/integration/ -v --tb=short

# ── Code Quality ──────────────────────────────────────────────────────────

lint: ## Run linter (ruff check)
	$(RUFF) check src/ tests/

format: ## Run formatter (ruff format)
	$(RUFF) format src/ tests/

# ── Infrastructure ────────────────────────────────────────────────────────

certs: ## Generate development mTLS certificates
	bash $(CERTS_DIR)/generate-dev-certs.sh

docker-up: ## Start all services via Docker Compose
	docker compose up -d

docker-down: ## Stop all Docker Compose services
	docker compose down

# ── Cleanup ───────────────────────────────────────────────────────────────

clean: ## Remove build artifacts, caches, and generated files
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf __pycache__
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -not -path "./.venv/*" -delete 2>/dev/null || true
	rm -rf *.egg-info
	rm -rf dist/ build/
	rm -rf data/
	rm -rf audit/
	rm -f $(CERTS_DIR)/ca.key $(CERTS_DIR)/ca.crt
	rm -f $(CERTS_DIR)/dso.key $(CERTS_DIR)/dso.crt
	rm -f $(CERTS_DIR)/aggregator.key $(CERTS_DIR)/aggregator.crt
	rm -f $(CERTS_DIR)/prosumer.key $(CERTS_DIR)/prosumer.crt
	rm -f $(CERTS_DIR)/catalog.key $(CERTS_DIR)/catalog.crt
	@echo "Clean complete."

# ── Help ──────────────────────────────────────────────────────────────────

help: ## Show this help message
	@echo "Federated Data Space - Development Commands"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
