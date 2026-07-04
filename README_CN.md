# DataFlowX — 智能数据流编排引擎

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/python-≥3.10-blue" alt="Python">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/status-active-brightgreen" alt="Status">
</p>

<p align="center">
  <strong>AI 驱动的 ETL 管道编排 · DAG 执行引擎 · 多源数据集成</strong>
</p>

---

## 目录

- [项目概述](#项目概述)
- [为什么选择 DataFlowX](#为什么选择-dataflowx)
- [系统架构](#系统架构)
- [核心功能](#核心功能)
- [快速开始](#快速开始)
- [管道配置](#管道配置)
- [内置任务](#内置任务)
- [CLI 参考](#cli-参考)
- [开发指南](#开发指南)
- [许可证](#许可证)

---

## 项目概述

**DataFlowX** 是一个智能数据流编排引擎，旨在简化复杂的 ETL（提取-转换-加载）管道。它提供了声明式 YAML 驱动的管道配置模型、支持自动并行执行的 DAG（有向无环图）执行引擎，以及丰富的内置任务注册表。

DataFlowX 属于 `github-big-proj` 系列，与 Industrial Data Bridge (proj1)、BioOmics Bridge (proj3)、PharmaGuard (proj4) 和 BridgeX (proj5) 并驾齐驱。它将该生态系统的数据管道能力扩展到通用可配置数据编排领域。

### 解决什么问题？

现代数据工作流涉及异构数据源（CSV、数据库、API、消息队列）、复杂的转换链和严格的依赖管理。DataFlowX 通过以下方式应对这些挑战：

- **声明式管道**：以 YAML 而非命令式代码定义管道。
- **自动 DAG 解析**：自动检测循环依赖、解析依赖关系并并行化独立任务。
- **内置任务库**：9+ 种任务类型（提取、转换、过滤、聚合、连接、增强、校验、自定义、加载）。
- **可扩展架构**：通过插件系统注册自定义任务和数据连接器。
- **可观测性**：实时执行状态、重试逻辑和故障恢复。

---

## 系统架构

```
┌──────────────────────────────────────────────────────┐
│                   DataFlowX CLI                       │
│  run  │  validate  │  list  │  visualize  │  watch   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│              Pipeline Orchestrator（管道编排器）       │
│  ┌─────────┐  ┌──────────┐  ┌───────────────────┐   │
│  │ 注册     │  │ 调度     │  │ 监控与恢复         │   │
│  └─────────┘  └──────────┘  └───────────────────┘   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│               Pipeline Engine (DAG)                   │
│  ┌────────────┐  ┌────────────┐  ┌──────────────┐   │
│  │ 拓扑排序   │  │ 并行执行   │  │ 节点执行     │   │
│  └────────────┘  └────────────┘  └──────────────┘   │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│                 Task Registry（任务注册表）             │
│  extract │ transform │ filter │ aggregate │ join     │
│  enrich  │ validate  │ custom │ load                 │
└───────────────────┬──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│              Data Connectors（数据连接器）              │
│  CSV │ PostgreSQL │ MySQL │ SQLite │ REST API        │
│  Redis │ Kafka │ S3 │ Parquet                       │
└──────────────────────────────────────────────────────┘
```

---

## 核心功能

### 基础能力

| 功能 | 说明 |
|------|------|
| **声明式 YAML 管道** | 以结构化 YAML 定义复杂数据流，支持配置校验 |
| **DAG 执行引擎** | 自动拓扑排序、循环检测和并行执行 |
| **任务注册表** | 9 种内置任务类型，支持参数化配置 |
| **多源连接器** | 7+ 种数据源连接器（CSV、PostgreSQL、MySQL、SQLite、REST、Redis、Parquet） |
| **管道编排器** | 注册、调度和监控多条管道 |
| **重试与恢复** | 可配置重试次数、超时和错误恢复策略 |
| **CLI 界面** | 功能完整的命令行界面（run、validate、list、visualize、watch） |
| **类型安全模型** | 基于 Pydantic v2 的管道配置和执行结果模型 |

### 高级功能

- **增量处理**：基于检查点的增量数据处理
- **管道可视化**：ASCII 艺术 DAG 渲染和 Graphviz 导出
- **监控模式**：文件系统监控器，数据变更时自动触发管道
- **插件系统**：通过入口点注册自定义任务和连接器

---

## 快速开始

### 环境要求

- Python 3.10+
- pip

### 安装

```bash
# 克隆仓库
git clone https://github.com/lanekingkong/github-big-proj6.git
cd github-big-proj6

# 开发模式安装
pip install -e .

# 或安装所有可选依赖
pip install -e ".[all]"
```

### 你的第一条管道

创建 `hello_pipeline.yaml`：

```yaml
name: hello_dataflow
version: "1.0"
description: 我的第一条 DataFlowX 管道

data_sources:
  - name: users_csv
    type: csv
    path: users.csv

tasks:
  - id: extract_users
    name: 提取用户数据
    type: extract
    input_source: users_csv

  - id: clean_names
    name: 清理姓名
    type: transform
    depends_on:
      - extract_users
    transforms:
      - column: name
        operation: strip
      - column: name
        operation: lower

  - id: filter_active
    name: 过滤活跃用户
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

执行：

```bash
dataflowx run hello_pipeline.yaml
```

---

## 管道配置

DataFlowX 管道由四个部分组成：

### 1. 数据源

定义输入连接器，包含类型、路径和选项：

```yaml
data_sources:
  - name: sales_db
    type: postgresql
    connection_string: postgresql://localhost:5432/sales
    query: SELECT * FROM orders WHERE date > '2026-01-01'
```

### 2. 任务

通过显式依赖声明定义处理 DAG：

```yaml
tasks:
  - id: load_orders
    name: 加载订单
    type: extract
    input_source: sales_db

  - id: enrich_orders
    name: 用客户数据增强订单
    type: enrich
    depends_on: [load_orders]
    enrich_source: customer_db
    join_key: customer_id
```

### 3. 转换与过滤

链式列级操作：

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

### 4. 数据出口

定义输出连接器：

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

## 内置任务

| 任务类型 | 说明 | 关键参数 |
|----------|------|----------|
| `extract` | 从数据源加载数据 | `input_source` |
| `transform` | 列级转换操作 | `transforms`（strip, cast, lower, upper, replace 等） |
| `filter` | 按条件过滤行 | `filters`（eq, neq, gt, gte, lt, lte, in, contains, regex） |
| `aggregate` | 分组聚合 | `group_by`, `aggregations`（count, sum, avg, min, max） |
| `join` | 连接两个数据集 | `join_source`, `join_key`, `join_type`（inner, left, right, outer） |
| `enrich` | 使用外部数据增强 | `enrich_source`, `join_key` |
| `validate` | 数据质量校验 | `validations`（not_null, unique, range, regex） |
| `custom` | 运行自定义 Python 代码 | `code`, `params` |
| `load` | 写入数据到出口 | `output_sink` |

---

## CLI 参考

```bash
# 运行管道
dataflowx run <pipeline.yaml>

# 校验管道（不执行）
dataflowx validate <pipeline.yaml>

# 列出已注册的管道
dataflowx list

# 可视化管道 DAG
dataflowx visualize <pipeline.yaml>
dataflowx visualize <pipeline.yaml> --output dag.png

# 监控目录变更并自动运行管道
dataflowx watch --dir ./data --pipeline pipeline.yaml
```

---

## 开发指南

### 项目结构

```
github_big_proj6/
├── src/
│   └── dataflowx/
│       ├── __init__.py          # 包导出
│       ├── cli.py               # CLI 入口
│       └── core/
│           ├── __init__.py      # 核心模块导出
│           ├── models.py        # Pydantic 数据模型
│           ├── engine.py        # PipelineEngine + DAG 执行器
│           └── registry.py      # 任务和连接器注册表
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_core.py             # 单元和集成测试
├── pyproject.toml               # 项目元数据和依赖
├── LICENSE                      # MIT 许可证
├── .gitignore
├── README.md                    # 英文文档
└── README_CN.md                 # 中文文档
```

### 运行测试

```bash
# 安装测试依赖
pip install -e ".[dev]"

# 运行所有测试
pytest tests/ -v

# 带覆盖率运行
pytest tests/ -v --cov=dataflowx --cov-report=html
```

### 贡献指南

1. Fork 仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 为变更编写测试
4. 运行测试套件 (`pytest tests/ -v`)
5. 提交 Pull Request

---

## 许可证

本项目基于 MIT 许可证发布。详见 [LICENSE](LICENSE)。

---

<p align="center">
  <strong>DataFlowX</strong> — <a href="https://github.com/lanekingkong">lanekingkong</a> 的 github-big-proj 生态系统项目
</p>
