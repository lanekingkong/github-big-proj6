"""
DataFlowX Core Models — Pydantic models for pipeline configuration and execution.

Defines the domain model: pipelines, tasks, data sources, data sinks,
transformations, and execution results.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator


class TaskType(str, Enum):
    """Supported task types."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    ENRICH = "enrich"
    VALIDATE = "validate"
    CUSTOM = "custom"


class ConnectorType(str, Enum):
    """Data source/sink connector types."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    SQL = "sql"
    HTTP = "http"
    KAFKA = "kafka"
    S3 = "s3"


class ExecutionStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class TransformRule(BaseModel):
    """A single transformation rule applied to a data column."""
    column: str = Field(..., description="Target column name.")
    operation: str = Field(..., description="Operation: rename, cast, drop, fillna, map, normalize, custom.")
    params: Dict[str, Any] = Field(default_factory=dict, description="Operation parameters.")


class FilterRule(BaseModel):
    """A filter rule for row-level filtering."""
    column: str
    operator: str = Field(..., description="Operator: eq, ne, gt, gte, lt, lte, in, contains, regex.")
    value: Any


class JoinRule(BaseModel):
    """Join configuration for merging datasets."""
    left_key: str
    right_key: str
    how: str = Field(default="inner", description="Join type: inner, left, right, outer.")
    right_source: str = Field(..., description="Name of the right data source.")


class TaskNode(BaseModel):
    """A single task in the pipeline DAG."""
    id: str = Field(..., description="Unique task identifier.")
    name: str = Field(..., description="Human-readable task name.")
    type: TaskType = Field(..., description="Task type.")
    depends_on: List[str] = Field(default_factory=list, description="Dependencies (task IDs).")
    input_source: Optional[str] = Field(default=None, description="Input data source name.")
    transforms: List[TransformRule] = Field(default_factory=list)
    filters: List[FilterRule] = Field(default_factory=list)
    join: Optional[JoinRule] = None
    aggregations: List[Dict[str, Any]] = Field(default_factory=list)
    custom_code: Optional[str] = Field(default=None, description="Custom Python code for CUSTOM tasks.")
    retry_count: int = Field(default=0, description="Max retries on failure.")
    timeout_seconds: int = Field(default=300)


class DataSource(BaseModel):
    """A data source configuration."""
    name: str
    type: ConnectorType
    path: Optional[str] = None
    connection_string: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class DataSink(BaseModel):
    """A data sink (output) configuration."""
    name: str
    type: ConnectorType
    path: Optional[str] = None
    connection_string: Optional[str] = None
    table_name: Optional[str] = None
    mode: str = Field(default="overwrite", description="Write mode: overwrite, append, merge.")
    options: Dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """Complete pipeline configuration."""
    name: str = Field(..., description="Pipeline name.")
    version: str = Field(default="1.0")
    description: str = Field(default="")
    schedule: Optional[str] = Field(default=None, description="Cron schedule expression.")
    data_sources: List[DataSource] = Field(default_factory=list)
    tasks: List[TaskNode] = Field(default_factory=list)
    data_sinks: List[DataSink] = Field(default_factory=list)
    global_settings: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_file(cls, file_path: str) -> "PipelineConfig":
        """Load pipeline configuration from YAML or JSON file."""
        path = Path(file_path)
        if path.suffix in (".yaml", ".yml"):
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
        elif path.suffix == ".json":
            import json
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")
        return cls(**data)

    def validate(self) -> List[str]:
        """Validate configuration consistency. Returns list of errors."""
        errors = []
        task_ids = {t.id for t in self.tasks}
        source_names = {s.name for s in self.data_sources}
        sink_names = {s.name for s in self.data_sinks}

        # Check dependencies exist
        for task in self.tasks:
            for dep in task.depends_on:
                if dep not in task_ids:
                    errors.append(f"Task '{task.id}' depends on unknown task '{dep}'")

        # Check source references
        for task in self.tasks:
            if task.input_source and task.input_source not in source_names:
                errors.append(f"Task '{task.id}' references unknown source '{task.input_source}'")

        # Check join references
        for task in self.tasks:
            if task.join and task.join.right_source not in source_names:
                errors.append(f"Task '{task.id}' join references unknown source '{task.join.right_source}'")

        # Check for cycles
        if self._has_cycle():
            errors.append("Pipeline DAG contains a cycle.")

        return errors

    def _has_cycle(self) -> bool:
        """Detect cycles in the task dependency graph."""
        import networkx as nx
        g = nx.DiGraph()
        for task in self.tasks:
            g.add_node(task.id)
            for dep in task.depends_on:
                g.add_edge(dep, task.id)
        try:
            cycles = list(nx.simple_cycles(g))
            return len(cycles) > 0
        except nx.NetworkXNoCycle:
            return False

    def topological_order(self) -> List[str]:
        """Return topologically sorted task IDs."""
        import networkx as nx
        g = nx.DiGraph()
        for task in self.tasks:
            g.add_node(task.id)
            for dep in task.depends_on:
                g.add_edge(dep, task.id)
        return list(nx.topological_sort(g))


class ExecutionResult(BaseModel):
    """Result of a pipeline execution."""
    pipeline_name: str
    status: ExecutionStatus
    tasks_executed: int = 0
    tasks_failed: int = 0
    rows_processed: int = 0
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    task_results: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
