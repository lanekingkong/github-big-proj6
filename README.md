# DataFlowX — Intelligent Data Flow Orchestration Engine

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/python-≥3.10-blue" alt="Python">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/status-active-brightgreen" alt="Status">
</p>

<p align="center">
  <strong>AI-Powered ETL Pipeline Orchestration · DAG Execution Engine · Multi-Source Data Integration</strong>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Why DataFlowX](#why-dataflowx)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Pipeline Configuration](#pipeline-configuration)
- [Built-In Tasks](#built-in-tasks)
- [CLI Reference](#cli-reference)
- [Development](#development)
- [License](#license)

---

## Overview

**DataFlowX** is an intelligent data flow orchestration engine designed to simplify complex ETL (Extract-Transform-Load) pipelines. It provides a declarative YAML-driven pipeline configuration model, a DAG (Directed Acyclic Graph) execution engine with automatic parallelism, and a rich built-in task registry for common data operations.

DataFlowX belongs to the `github-big-proj` family of AI-powered infrastructure tools, sitting alongside Industrial Data Bridge (proj1), BioOmics Bridge (proj3), PharmaGuard (proj4), and BridgeX (proj5). It extends the data pipeline capabilities of this ecosystem with a focus on general-purpose, configurable data orchestration.

### What Problem Does It Solve?

Modern data workflows involve heterogeneous sources (CSV, databases, APIs, message queues), complex transformation chains, and strict dependency management. DataFlowX addresses these challenges by:

- **Declarative Pipelines**: Define pipelines as YAML, not imperative code.
- **Automatic DAG Resolution**: Detect cycles, resolve dependencies, and parallelize independent tasks.
- **Built-In Task Library**: 9+ task types (extract, transform, filter, aggregate, join, enrich, validate, custom, load).
- **Extensible Architecture**: Register custom tasks and data connectors via a plugin system.
- **Observability**: Real-time execution status, retry logic, and error recovery.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   DataFlowX CLI                       │
│  run  │  validate  │  list  │  visualize  │  watch   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│              Pipeline Orchestrator                    │
│  ┌─────────┐  ┌──────────┐  ┌───────────────────┐   │
│  │ Register │  │ Schedule │  │ Monitor & Recover │   │
│  └─────────┘  └──────────┘  └───────────────────┘   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│               Pipeline Engine (DAG)                   │
│  ┌────────────┐  ┌────────────┐  ┌──────────────┐   │
│  │ Topo Sort  │  │ Parallelize│  │ Execute Node │   │
│  └────────────┘  └────────────┘  └──────────────┘   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│                 Task Registry                         │
│  extract │ transform │ filter │ aggregate │ join     │
│  enrich  │ validate  │ custom │ load                 │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│              Data Connectors                          │
│  CSV │ PostgreSQL │ MySQL │ SQLite │ REST API        │
│  Redis │ Kafka │ S3 │ Parquet                       │
└──────────────────────────────────────────────────────┘
```

---

## Features

### Core Capabilities

| Feature | Description |
|---------|-------------|
| **Declarative YAML Pipelines** | Define complex data flows as structured YAML with validation |
| **DAG Execution Engine** | Automatic topological sort, cycle detection, and parallel execution |
| **Task Registry** | 9 built-in task types with configurable parameters |
| **Multi-Source Connectors** | 7+ data source connectors (CSV, PostgreSQL, MySQL, SQLite, REST, Redis, Parquet) |
| **Pipeline Orchestrator** | Register, schedule, and monitor multiple pipelines |
| **Retry & Recovery** | Configurable retry counts, timeout, and error recovery strategies |
| **CLI Interface** | Full-featured CLI for pipeline management (run, validate, list, visualize, watch) |
| **Type-Safe Models** | Pydantic v2 models for pipeline configuration and execution results |

### Advanced

- **Incremental Processing**: Checkpoint-based incremental data processing
- **Pipeline Visualization**: ASCII art DAG rendering and Graphviz export
- **Watch Mode**: File-system watcher to auto-trigger pipelines on data changes
- **Plugin System**: Register custom tasks and connectors via entry points

---

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/lanekingkong/github-big-proj6.git
cd github-big-proj6

# Install in development mode
pip install -e .

# Or install with all optional dependencies
pip install -e ".[all]"
```

### Your First Pipeline

Create `hello_pipeline.yaml`:

```yaml
name: hello_dataflow
version: "1.0"
description: My first DataFlowX pipeline

data_sources:
  - name: users_csv
    type: csv
    path: users.csv

tasks:
  - id: extract_users
    name: Extract Users
    type: extract
    input_source: users_csv

  - id: clean_names
    name: Clean Names
    type: transform
    depends_on:
      - extract_users
    transforms:
      - column: name
        operation: strip
      - column: name
        operation: lower

  - id: filter_active
    name: Filter Active Users
    type: filter
    depends_on:
      - clean_names
    filters:
      - column: status
        operator: eq
        value: active

data_sinks:
  - name: output_csv
    type: csv
    path: active_users.csv
    mode: overwrite
```

Run it:

```bash
dataflowx run hello_pipeline.yaml
```

---

## Pipeline Configuration

A DataFlowX pipeline consists of four sections:

### 1. Data Sources

Define input connectors with type, path, and options:

```yaml
data_sources:
  - name: sales_db
    type: postgresql
    connection_string: postgresql://localhost:5432/sales
    query: SELECT * FROM orders WHERE date > '2026-01-01'
```

### 2. Tasks

Define the processing DAG with explicit dependency declarations:

```yaml
tasks:
  - id: load_orders
    name: Load Orders
    type: extract
    input_source: sales_db

  - id: enrich_orders
    name: Enrich Orders with Customer Data
    type: enrich
    depends_on: [load_orders]
    enrich_source: customer_db
    join_key: customer_id
```

### 3. Transforms & Filters

Chain column-level operations:

```yaml
transforms:
  - column: price
    operation: cast
    params:
      dtype: float
  - column: date
    operation: cast
    params:
      dtype: datetime
      format: "%Y-%m-%d"

filters:
  - column: price
    operator: gte
    value: 100
  - column: category
    operator: in
    value: [electronics, books]
```

### 4. Data Sinks

Define output connectors:

```yaml
data_sinks:
  - name: enriched_csv
    type: csv
    path: enriched_orders.csv
  - name: analytics_db
    type: postgresql
    connection_string: postgresql://localhost:5432/analytics
    table: enriched_orders
    mode: append
```

---

## Built-In Tasks

| Task Type | Description | Key Parameters |
|-----------|-------------|----------------|
| `extract` | Load data from a source | `input_source` |
| `transform` | Apply column-level transformations | `transforms` (strip, cast, lower, upper, replace, etc.) |
| `filter` | Filter rows by conditions | `filters` (eq, neq, gt, gte, lt, lte, in, contains, regex) |
| `aggregate` | Group and aggregate | `group_by`, `aggregations` (count, sum, avg, min, max) |
| `join` | Join two datasets | `join_source`, `join_key`, `join_type` (inner, left, right, outer) |
| `enrich` | Enrich with external data | `enrich_source`, `join_key` |
| `validate` | Validate data quality | `validations` (not_null, unique, range, regex) |
| `custom` | Run arbitrary Python code | `code`, `params` |
| `load` | Write data to a sink | `output_sink` |

---

## CLI Reference

```bash
# Run a pipeline
dataflowx run <pipeline.yaml>

# Validate a pipeline without executing
dataflowx validate <pipeline.yaml>

# List registered pipelines
dataflowx list

# Visualize pipeline DAG
dataflowx visualize <pipeline.yaml>
dataflowx visualize <pipeline.yaml> --output dag.png

# Watch a directory for changes and auto-run pipeline
dataflowx watch --dir ./data --pipeline pipeline.yaml
```

---

## Development

### Project Structure

```
github_big_proj6/
├── src/
│   └── dataflowx/
│       ├── __init__.py          # Package exports
│       ├── cli.py               # CLI entry point
│       └── core/
│           ├── __init__.py      # Core module exports
│           ├── models.py        # Pydantic data models
│           ├── engine.py        # PipelineEngine + DAG executor
│           └── registry.py      # Task & connector registry
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_core.py             # Unit & integration tests
├── pyproject.toml               # Project metadata & dependencies
├── LICENSE                      # MIT License
├── .gitignore
├── README.md                    # English documentation
└── README_CN.md                 # Chinese documentation
```

### Running Tests

```bash
# Install test dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=dataflowx --cov-report=html
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Run the test suite (`pytest tests/ -v`)
5. Submit a pull request

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>DataFlowX</strong> — Part of the github-big-proj ecosystem by <a href="https://github.com/lanekingkong">lanekingkong</a>
</p>
