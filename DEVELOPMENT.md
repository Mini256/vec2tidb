# Development Guide

## Prerequisites

- Python 3.10+
- Docker (for local database testing)
- Git
- uv (for package management)

## Install dependencies

```bash
git clone https://github.com/Mini256/vec2tidb.git
cd vec2tidb
uv sync
```

## Local Database Setup

### Setup TiDB database

```bash
# Install TiUP
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh

# Start TiDB Playground cluster
tiup playground

# Test if TiDB is working
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 'Hello, World!';"
# +---------------+
# | Hello, World! |
# +---------------+
# | Hello, World! |
# +---------------+
```

### Setup Qdrant database

```bash
# Start Qdrant
docker run -d --name qdrant-local \
  -p 6333:6333 \
  -p 6334:6334 \
  qdrant/qdrant:latest

# Test if Qdrant is working
curl http://localhost:6333/collections
```

## Testing

```bash
# Run all tests
make test

# Run tests with coverage
make test-cov

# Run specific test
uv run pytest tests/test_qdrant.py::test_migrate_create_mode -v
```

## Development Commands

```bash
# Format code
make format

# Run linting
make lint

# Run all checks
make check

# Clean generated files
make clean
```

## Resources

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [Qdrant Documentation](https://qdrant.tech/documentation/)
- [Click Documentation](https://click.palletsprojects.com/) 