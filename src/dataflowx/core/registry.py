"""
DataFlowX Task Registry — Built-in task implementations and connector registry.

Provides a catalog of executable tasks (extract, transform, filter, aggregate,
join, enrich, validate, custom) and data source/sink connectors.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional

import pandas as pd

from dataflowx.core.models import (
    TaskNode,
    DataSource,
    DataSink,
    TaskType,
    ConnectorType,
    TransformRule,
    FilterRule,
)


class TaskRegistry:
    """Registry of built-in task implementations and connectors."""

    def list_tasks(self) -> list[dict]:
        """Return all available built-in tasks."""
        return [
            {"name": "extract", "category": "ingestion", "description": "Extract data from a source connector."},
            {"name": "transform", "category": "processing", "description": "Apply column-level transformations (rename, cast, map, normalize)."},
            {"name": "filter", "category": "processing", "description": "Filter rows by column conditions."},
            {"name": "aggregate", "category": "processing", "description": "Group-by aggregation with sum/mean/count/min/max."},
            {"name": "join", "category": "processing", "description": "Merge/join two datasets on key columns."},
            {"name": "enrich", "category": "processing", "description": "Enrich data with external lookups or AI-generated fields."},
            {"name": "validate", "category": "quality", "description": "Validate data against schema or business rules."},
            {"name": "custom", "category": "extensibility", "description": "Run user-provided Python code."},
            {"name": "load", "category": "sink", "description": "Load data into a destination connector."},
        ]

    def list_connectors(self) -> list[dict]:
        """Return all available data connectors."""
        return [
            {"name": "csv", "type": "file", "description": "Read/write CSV files."},
            {"name": "json", "type": "file", "description": "Read/write JSON/JSONL files."},
            {"name": "parquet", "type": "file", "description": "Read/write Parquet files."},
            {"name": "sql", "type": "database", "description": "SQL database via SQLAlchemy."},
            {"name": "http", "type": "api", "description": "REST API via HTTP requests."},
            {"name": "kafka", "type": "streaming", "description": "Apache Kafka streaming."},
            {"name": "s3", "type": "cloud", "description": "AWS S3 / MinIO object storage."},
        ]

    # ─── Source / Sink Connectors ───────────────────────────────────────────

    def load_source(self, source: DataSource) -> pd.DataFrame:
        """Load data from a configured data source."""
        if source.type == ConnectorType.CSV:
            return pd.read_csv(source.path, **source.options)
        elif source.type == ConnectorType.JSON:
            return pd.read_json(source.path, **source.options)
        elif source.type == ConnectorType.PARQUET:
            return pd.read_parquet(source.path, **source.options)
        elif source.type == ConnectorType.SQL:
            from sqlalchemy import create_engine
            engine = create_engine(source.connection_string)
            return pd.read_sql(source.query or "SELECT * FROM data", engine, **source.options)
        elif source.type == ConnectorType.HTTP:
            import httpx
            resp = httpx.get(source.path or "", **source.options)
            resp.raise_for_status()
            return pd.DataFrame(resp.json())
        else:
            raise ValueError(f"Unsupported source connector: {source.type}")

    def write_sink(self, sink: DataSink, data: pd.DataFrame):
        """Write data to a configured data sink."""
        if sink.type == ConnectorType.CSV:
            data.to_csv(sink.path, index=False, **sink.options)
        elif sink.type == ConnectorType.JSON:
            data.to_json(sink.path, orient="records", **sink.options)
        elif sink.type == ConnectorType.PARQUET:
            data.to_parquet(sink.path, index=False, **sink.options)
        elif sink.type == ConnectorType.SQL:
            from sqlalchemy import create_engine
            engine = create_engine(sink.connection_string)
            if_exists = sink.mode if sink.mode in ("append", "replace", "fail") else "replace"
            data.to_sql(sink.table_name or "output", engine, if_exists=if_exists, index=False, **sink.options)
        else:
            raise ValueError(f"Unsupported sink connector: {sink.type}")

    # ─── Task Executors ─────────────────────────────────────────────────────

    def execute_extract(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Extract task - acts as entry point, delegates to source loader."""
        return input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()

    def execute_transform(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Apply column-level transformations."""
        df = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        for rule in task.transforms:
            op = rule.operation
            col = rule.column
            params = rule.params

            if op == "rename":
                new_name = params.get("new_name", col)
                df = df.rename(columns={col: new_name})
            elif op == "cast":
                dtype = params.get("dtype", "str")
                df[col] = df[col].astype(dtype)
            elif op == "drop":
                df = df.drop(columns=[col], errors="ignore")
            elif op == "fillna":
                value = params.get("value", 0)
                df[col] = df[col].fillna(value)
            elif op == "map":
                mapping = params.get("mapping", {})
                df[col] = df[col].map(mapping).fillna(df[col])
            elif op == "normalize":
                # Min-max normalization
                if col in df.columns:
                    vmin, vmax = df[col].min(), df[col].max()
                    if vmax > vmin:
                        df[col] = (df[col] - vmin) / (vmax - vmin)
            elif op == "custom":
                expr = params.get("expression", "")
                if expr:
                    df[col] = df.eval(expr)
            elif op == "lowercase":
                df[col] = df[col].str.lower()
            elif op == "strip":
                df[col] = df[col].str.strip()
            elif op == "extract_regex":
                pattern = params.get("pattern", "")
                df[col] = df[col].str.extract(pattern)
        return df

    def execute_filter(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Filter rows based on conditions."""
        df = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        for rule in task.filters:
            col = rule.column
            op = rule.operator
            val = rule.value
            if col not in df.columns:
                continue
            if op == "eq":
                df = df[df[col] == val]
            elif op == "ne":
                df = df[df[col] != val]
            elif op == "gt":
                df = df[df[col] > val]
            elif op == "gte":
                df = df[df[col] >= val]
            elif op == "lt":
                df = df[df[col] < val]
            elif op == "lte":
                df = df[df[col] <= val]
            elif op == "in":
                df = df[df[col].isin(val if isinstance(val, list) else [val])]
            elif op == "contains":
                df = df[df[col].astype(str).str.contains(str(val), na=False)]
            elif op == "regex":
                df = df[df[col].astype(str).str.match(str(val), na=False)]
        return df

    def execute_aggregate(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Apply group-by aggregations."""
        df = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        if not task.aggregations:
            return df

        group_cols = []
        agg_specs: Dict[str, str] = {}
        for agg in task.aggregations:
            col = agg.get("column", "")
            func = agg.get("function", "count")
            alias = agg.get("alias", f"{col}_{func}")
            group_by = agg.get("group_by", False)
            if group_by:
                group_cols.append(col)
            else:
                agg_specs[alias] = (col, func)

        if group_cols and agg_specs:
            agg_dict = {alias: (col, func) for alias, (col, func) in agg_specs.items()}
            return df.groupby(group_cols).agg(**{k: pd.NamedAgg(column=v[0], aggfunc=v[1]) for k, v in agg_dict.items()}).reset_index()
        return df

    def execute_join(self, task: TaskNode, input_data: Any, right_data: Any) -> pd.DataFrame:
        """Join two datasets."""
        left = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        right = right_data if isinstance(right_data, pd.DataFrame) else pd.DataFrame()
        if task.join:
            return left.merge(right, left_on=task.join.left_key, right_on=task.join.right_key, how=task.join.how)
        return left

    def execute_enrich(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Enrich data with external lookups or computed fields."""
        df = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        # Simple enrichment: add computed columns from transforms
        return self.execute_transform(task, df)

    def execute_validate(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Validate data against rules. Returns data with validation columns."""
        df = input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
        for rule in task.transforms:
            col = rule.column
            if col not in df.columns:
                continue
            # Add validation flags
            if rule.operation == "not_null":
                df[f"{col}_valid"] = df[col].notna()
            elif rule.operation == "unique":
                df[f"{col}_valid"] = ~df[col].duplicated(keep=False)
            elif rule.operation == "range":
                lo = rule.params.get("min")
                hi = rule.params.get("max")
                if lo is not None and hi is not None:
                    df[f"{col}_valid"] = df[col].between(lo, hi)
        return df

    def execute_custom(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Execute user-provided custom Python code."""
        if task.custom_code:
            local_vars = {"df": input_data, "pd": pd, "result": None}
            exec(task.custom_code, {"__builtins__": {}}, local_vars)
            result = local_vars.get("result", local_vars.get("df"))
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        return input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()

    def execute_load(self, task: TaskNode, input_data: Any) -> pd.DataFrame:
        """Load task - pass-through for sink writing."""
        return input_data if isinstance(input_data, pd.DataFrame) else pd.DataFrame()
