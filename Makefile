# NEDL ETL Makefile
# =================
# Run `make setup` after cloning to install dependencies and hooks

.PHONY: setup install lint format typecheck test ci clean

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

