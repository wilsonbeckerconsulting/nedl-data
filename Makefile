# NEDL ETL Makefile
# =================
# Run `make setup` after cloning to install dependencies and hooks

.PHONY: setup install lint format typecheck test ci clean extract transform validate backfill pipeline

# First-time setup (run after clone)
setup: install
	pip install pre-commit
	pre-commit install
	@echo "✅ Setup complete! Pre-commit hooks installed."

# Install dependencies
install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

# Lint (check only)
lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

# Format (auto-fix)
format:
	ruff check --fix src/ tests/
	ruff format src/ tests/

# Type check
typecheck:
	pyright src/

# Run tests
test:
	pytest tests/ -v

# Full CI check (lint + typecheck + test)
ci: lint typecheck test

# Clean up
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +

# =============================================================================
# ETL Commands
# =============================================================================
# Usage:
#   make extract                              # yesterday's data
#   make extract START=2025-01-01             # from date to today
#   make extract START=2025-01-01 END=2025-01-31
#   make transform
#   make validate
#   make backfill START=2024-01 END=2024-12

# Build args string only if START/END are set
EXTRACT_ARGS := $(if $(START),--start-date $(START)) $(if $(END),--end-date $(END))
BACKFILL_ARGS := $(if $(START),--start $(START)) $(if $(END),--end $(END))

extract:
	python src/flows/extract.py $(EXTRACT_ARGS)

transform:
	python src/flows/transform_analytics.py

validate:
	python src/flows/validate.py

backfill:
	python scripts/backfill.py $(BACKFILL_ARGS)

# Full pipeline: extract → transform → validate
# Usage:
#   make pipeline                              # yesterday's data
#   make pipeline START=2025-01-01             # from date to today
#   make pipeline START=2025-01-01 END=2025-01-31
pipeline: extract transform validate
	@echo "✅ Pipeline complete!"

