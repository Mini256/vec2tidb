.PHONY: help install install-dev test format lint clean

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in development mode
	uv sync
	uv run pip install -e .

install-dev: ## Install development dependencies
	uv sync --group dev
	uv run pre-commit install

test: ## Run tests
	uv run pytest tests/ -v

test-cov: ## Run tests with coverage
	uv run pytest tests/ --cov=vec2tidb --cov-report=html --cov-report=term-missing

format: ## Format code with black and isort
	uv run black src/ tests/ examples/
	uv run isort src/ tests/ examples/

lint: ## Run linting checks
	uv run flake8 src/ tests/ examples/
	uv run black --check src/ tests/ examples/
	uv run isort --check-only src/ tests/ examples/

clean: ## Clean up generated files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage

check: format lint test ## Run all checks (format, lint, test)

build: ## Build the package
	uv run python -m build

publish: ## Publish to PyPI (requires proper setup)
	uv run python -m twine upload dist/*

start-tidb: ## Start local TiDB instance using Docker
	tiup playground

start-qdrant: ## Start local Qdrant instance using Docker
	docker run -d --name qdrant-local \
		-p 6333:6333 \
		-p 6334:6334 \
		qdrant/qdrant:latest

stop-qdrant: ## Stop and remove local Qdrant instance
	docker stop qdrant-local || true
	docker rm qdrant-local || true

test-load-sample: ## Test load sample data
	@uv run vec2tidb qdrant load-sample \
		--qdrant-collection-name vec2tidb_test \
		--dataset qdrant-docs

test-migration: ## Test migration with local databases (requires start-dbs first)
	@uv run vec2tidb qdrant migrate \
		--qdrant-collection-name vec2tidb_test \
		--mode create \
		--drop-table

test-benchmark: ## Test benchmark with local databases (requires start-dbs first)
	@uv run vec2tidb qdrant benchmark \
		--qdrant-collection-name vec2tidb_test \
		--workers 1,2,4,8 \
		--batch-sizes 100,200,400 \
		--table-prefix vec2tidb_test \
		--cleanup-tables
