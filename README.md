# vec2tidb

A CLI tool for migrating vector data from vector databases to TiDB.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

```
┌──────────────────┐    ┌────────────────┐    ┌────────────┐
│ Vector Database  │───▶│  vec2tidb CLI  │───▶│    TiDB    │
└──────────────────┘    └────────────────┘    └────────────┘
```

Supported vector databases:

- Qdrant


## Why migrate from vector database to TiDB?

TiDB is an open-source, distributed SQL database for modern AI applications:

- 🗄️ **Unified storage**: Store vector embeddings, documents, knowledge graphs, and operational data in a single database to reduce maintenance overhead.
- 🔍 **Native SQL support**: Run complex queries with full SQL capabilities, including joins, subqueries, aggregations, and advanced analytics.
- 📈 **Effortless scalability**: Scale out horizontally with ease to handle dynamic and growing workloads.
- 🔒 **Strong consistency**: Ensure data integrity and reliability with ACID transactions and strong consistency guarantees.

## Installation

To install the latest version, you can use the following command:

```bash
pip install vec2tidb
```

## Usage

To show all commands, use the following command:

```bash
vec2tidb --help
```

### Qdrant Commands

To show all `qdrant` subcommands, use the following command:

```bash
vec2tidb qdrant --help
```

> [!NOTE]
> 
> It is recommended to use `qdrant dump` to export the vector data to CSV file, upload the CSV file to S3 (or other cloud storage), and then use Import feature in TiDB Cloud to import the data to TiDB.

#### Command: `qdrant migrate`

To migrate vectors from Qdrant collection to a new TiDB table, use `create` mode.

```bash
vec2tidb qdrant migrate \
  --qdrant-api-url http://localhost:6333 \
  --qdrant-collection-name test_collection \
  --tidb-database-url mysql+pymysql://root:@localhost:4000/test
```

To migrate the vectors from Qdrant collection to an existing TiDB table, use `update` mode.

```bash
vec2tidb qdrant migrate \
  --qdrant-api-url http://localhost:6333 \
  --qdrant-collection-name test_collection \
  --tidb-database-url mysql+pymysql://root:@localhost:4000/test \
  --mode update \
  --table-name test_table \
  --id-column id \
  --vector-column vector \
  --payload-column payload
```

**Command Options**

| Option                     | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `--mode`                   | Migration mode: `create` (create new table) or `update` (update existing table by ID). Default: `create` |
| `--qdrant-api-url`         | Qdrant API endpoint. Default: `http://localhost:6333`                                           |
| `--qdrant-api-key`         | Qdrant API key (if authentication is enabled)                                                    |
| `--qdrant-collection-name` | Name of the source Qdrant collection (required)                                                  |
| `--tidb-database-url`      | TiDB connection string. Default: `mysql+pymysql://root:@localhost:4000/test`                    |
| `--table-name`             | Target TiDB table name. Required in update mode; defaults to collection name in create mode     |
| `--id-column`              | ID column name in TiDB table. Required in update mode; default: `id` in create mode            |
| `--id-column-type`         | ID column type in TiDB table. Default: `BIGINT`                                                 |
| `--vector-column`          | Vector column name in TiDB table. Required in update mode; default: `vector` in create mode    |
| `--payload-column`         | Payload column name in TiDB table. Optional in update mode; default: `payload` in create mode  |
| `--batch-size`             | Batch size for migration. Default: `100`                                                        |
| `--workers`                | Number of concurrent workers for migration. Default: `1`                                        |
| `--drop-table`             | Drop the target table if it exists (flag)                                                       |

**Environment Variables:**

The following options can also be set via environment variables:

| Variable                   | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `QDRANT_API_URL`           | Qdrant API endpoint. Default: `http://localhost:6333`                                           |
| `QDRANT_API_KEY`           | Qdrant API key (if authentication is enabled)                                                    |
| `QDRANT_COLLECTION_NAME`   | Qdrant collection name                                                                           |
| `TIDB_DATABASE_URL`        | TiDB connection string. Default: `mysql+pymysql://root:@localhost:4000/test`                    |

For example:

```bash
export QDRANT_API_URL="http://localhost:6333"
export QDRANT_API_KEY="your-api-key"
export QDRANT_COLLECTION_NAME="my_collection"
export TIDB_DATABASE_URL="mysql+pymysql://root:@localhost:4000/test"
```

#### Command: `qdrant load-sample`

To load a sample dataset into Qdrant collection.

```bash
vec2tidb qdrant load-sample \
  --qdrant-api-url http://localhost:6333 \
  --qdrant-collection-name sample_collection \
  --dataset midlib
```

**Command Options**

| Option                     | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `--qdrant-api-url`         | Qdrant API endpoint. Default: `http://localhost:6333`                                           |
| `--qdrant-api-key`         | Qdrant API key (if authentication is enabled)                                                    |
| `--qdrant-collection-name` | Name of the target Qdrant collection (required)                                                  |
| `--dataset`                | Sample dataset to load: `midlib`, `qdrant-docs`, `prefix-cache`. Default: `midlib` (required) |
| `--snapshot-uri`           | Custom snapshot URI (auto-determined from dataset if not provided)                              |

#### Command: `qdrant dump`

Export Qdrant collection data to CSV format with optimized performance.

```bash
vec2tidb qdrant dump \
  --qdrant-collection-name my_collection \
  --batch-size 200 \
  --no-payload \
  --buffer-size 50000 \
  --output-file export.csv
```

**Example with custom headers:**

```bash
vec2tidb qdrant dump \
  --qdrant-collection-name my_collection \
  --output-file export.csv \
  --id-header "record_id" \
  --vector-header "embedding" \
  --payload-header "metadata"
```

**Command Options**

| Option                     | Description                                    |
|----------------------------|------------------------------------------------|
| `--qdrant-collection-name` | Qdrant collection name (required)              |
| `--qdrant-api-url`         | Qdrant API endpoint. Default: `http://localhost:6333` |
| `--qdrant-api-key`         | Qdrant API key (if authentication is enabled)  |
| `--output-file`            | Output CSV file path (required)                |
| `--limit`                  | Maximum number of records to export            |
| `--offset`                 | Number of records to skip before starting export |
| `--no-vectors`             | Exclude vector data from export                |
| `--no-payload`             | Exclude payload data from export               |
| `--batch-size`             | Batch size for processing (default: 500)       |
| `--buffer-size`            | File buffer size in bytes (default: 10000)     |
| `--id-header`              | Custom header name for ID column (default: `id`) |
| `--vector-header`          | Custom header name for vector column (default: `vector`) |
| `--payload-header`         | Custom header name for payload column (default: `payload`) |


#### Command: `qdrant benchmark`

To run performance benchmarks with different configurations.

```bash
vec2tidb qdrant benchmark \
  --qdrant-api-url http://localhost:6333 \
  --qdrant-collection-name test_collection \
  --tidb-database-url mysql+pymysql://root:@localhost:4000/test \
  --dataset midlib \
  --workers 1,2,4 \
  --batch-sizes 100,500
```

**Command Options**

| Option                     | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `--qdrant-api-url`         | Qdrant API endpoint. Default: `http://localhost:6333`                                           |
| `--qdrant-api-key`         | Qdrant API key (if authentication is enabled)                                                    |
| `--qdrant-collection-name` | Name of the source Qdrant collection (required)                                                  |
| `--tidb-database-url`      | TiDB connection string. Default: `mysql+pymysql://root:@localhost:4000/test`                    |
| `--dataset`                | Auto-load sample dataset: `midlib`, `qdrant-docs`, `prefix-cache`                |
| `--snapshot-uri`           | Custom snapshot URI for auto-loading data (overrides --dataset)                                 |
| `--workers`                | Comma-separated list of worker counts to test. Default: `1,2,4,8`                               |
| `--batch-sizes`            | Comma-separated list of batch sizes to test. Default: `100,500,1000`                           |
| `--table-prefix`           | Prefix for benchmark table names. Default: `benchmark_test`                                     |


### TiDB Commands

To show all `tidb` subcommands, use the following command:

```bash
vec2tidb tidb --help
```

#### Command: `tidb batch-update`

Batch update target table with data from source table based on ID matching. This command supports efficient batch processing with pagination.

```bash
vec2tidb tidb batch-update \
  --source-table company_vectors \
  --source-id-column id \
  --target-table company_data \
  --target-id-column vector_hash \
  --column-mapping "vector:embedding" \
  --batch-size 5000
```

**Command Options**

| Option                     | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `--tidb-database-url`      | TiDB connection string. Default: `mysql+pymysql://root:@localhost:4000/test`                    |
| `--source-table`           | Source table name (required)                                                                     |
| `--source-id-column`       | ID column name in source table (required)                                                        |
| `--target-table`           | Target table name (required)                                                                     |
| `--target-id-column`       | ID column name in target table (required)                                                        |
| `--column-mapping`         | Column mapping in format 'source_col1:target_col1,source_col2:target_col2' (required)          |
| `--batch-size`             | Batch size for processing (default: 5000)                                                        |
| `--compact`                | Execute `ALTER TABLE COMPACT` on target table before updating (flag)                              |

## Development

For development setup and contribution guidelines, see [DEVELOPMENT.md](DEVELOPMENT.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
