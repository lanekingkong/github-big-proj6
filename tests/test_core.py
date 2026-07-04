"""Tests for DataFlowX - Pipeline models, engine, and registry."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from dataflowx.core.models import (
    PipelineConfig,
    TaskNode,
    DataSource,
    DataSink,
    TransformRule,
    FilterRule,
    ExecutionStatus,
    TaskType,
    ConnectorType,
)
from dataflowx.core.engine import PipelineEngine, PipelineOrchestrator, PipelineDAG
from dataflowx.core.registry import TaskRegistry


# ─── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def sample_config_yaml():
    """A minimal but valid pipeline config YAML."""
    return {
        "name": "test_pipeline",
        "version": "1.0",
        "description": "Test pipeline for unit tests",
        "data_sources": [
            {"name": "input_csv", "type": "csv", "path": "test_data.csv", "options": {}}
        ],
        "tasks": [
            {
                "id": "extract_data",
                "name": "Extract CSV",
                "type": "extract",
                "input_source": "input_csv",
            },
            {
                "id": "clean_data",
                "name": "Clean Data",
                "type": "transform",
                "depends_on": ["extract_data"],
                "transforms": [
                    {"column": "name", "operation": "strip"},
                    {"column": "age", "operation": "cast", "params": {"dtype": "int"}},
                ],
            },
            {
                "id": "filter_adults",
                "name": "Filter Adults",
                "type": "filter",
                "depends_on": ["clean_data"],
                "filters": [
                    {"column": "age", "operator": "gte", "value": 18},
                ],
            },
        ],
        "data_sinks": [
            {"name": "output_csv", "type": "csv", "path": "output.csv", "mode": "overwrite"}
        ],
    }


@pytest.fixture
def yaml_config_file(sample_config_yaml):
    """Write sample config to a temp YAML file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(sample_config_yaml, f)
        return f.name


# ─── Model Tests ─────────────────────────────────────────────────────────

class TestPipelineConfig:
    """Tests for PipelineConfig model."""

    def test_load_from_dict(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        assert config.name == "test_pipeline"
        assert len(config.tasks) == 3
        assert len(config.data_sources) == 1
        assert len(config.data_sinks) == 1

    def test_topological_order(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        order = config.topological_order()
        assert order[0] == "extract_data"
        assert order[1] == "clean_data"
        assert order[2] == "filter_adults"

    def test_validate_no_errors(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        errors = config.validate()
        assert len(errors) == 0

    def test_validate_missing_dependency(self, sample_config_yaml):
        sample_config_yaml["tasks"][1]["depends_on"] = ["nonexistent"]
        config = PipelineConfig(**sample_config_yaml)
        errors = config.validate()
        assert len(errors) > 0
        assert any("nonexistent" in e for e in errors)

    def test_cycle_detection(self):
        config = PipelineConfig(
            name="cycle_test",
            tasks=[
                TaskNode(id="A", name="A", type=TaskType.EXTRACT, depends_on=["B"]),
                TaskNode(id="B", name="B", type=TaskType.TRANSFORM, depends_on=["A"]),
            ],
        )
        assert config._has_cycle()

    def test_no_cycle(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        assert not config._has_cycle()


class TestTaskNode:
    """Tests for TaskNode model."""

    def test_default_retry_count(self):
        task = TaskNode(id="test", name="Test", type=TaskType.EXTRACT)
        assert task.retry_count == 0

    def test_custom_timeout(self):
        task = TaskNode(id="test", name="Test", type=TaskType.TRANSFORM, timeout_seconds=600)
        assert task.timeout_seconds == 600


# ─── Registry Tests ─────────────────────────────────────────────────────

class TestTaskRegistry:
    """Tests for TaskRegistry."""

    def setup_method(self):
        self.registry = TaskRegistry()

    def test_list_tasks(self):
        tasks = self.registry.list_tasks()
        assert len(tasks) >= 8
        assert any(t["name"] == "transform" for t in tasks)

    def test_list_connectors(self):
        connectors = self.registry.list_connectors()
        assert len(connectors) >= 5
        assert any(c["name"] == "csv" for c in connectors)

    def test_execute_transform_strip(self):
        import pandas as pd
        df = pd.DataFrame({"name": [" Alice ", " Bob "]})
        task = TaskNode(
            id="strip",
            name="Strip",
            type=TaskType.TRANSFORM,
            transforms=[TransformRule(column="name", operation="strip")],
        )
        result = self.registry.execute_transform(task, df)
        assert result["name"].tolist() == ["Alice", "Bob"]

    def test_execute_filter_gte(self):
        import pandas as pd
        df = pd.DataFrame({"age": [15, 20, 25, 10]})
        task = TaskNode(
            id="filter",
            name="Filter",
            type=TaskType.FILTER,
            filters=[FilterRule(column="age", operator="gte", value=18)],
        )
        result = self.registry.execute_filter(task, df)
        assert len(result) == 2
        assert result["age"].tolist() == [20, 25]

    def test_execute_transform_cast_int(self):
        import pandas as pd
        df = pd.DataFrame({"age": ["15", "20", "25"]})
        task = TaskNode(
            id="cast",
            name="Cast",
            type=TaskType.TRANSFORM,
            transforms=[TransformRule(column="age", operation="cast", params={"dtype": "int"})],
        )
        result = self.registry.execute_transform(task, df)
        assert result["age"].dtype == "int64"


# ─── Engine Tests ───────────────────────────────────────────────────────

class TestPipelineDAG:
    """Tests for PipelineDAG."""

    def test_get_task(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        dag = PipelineDAG(config)
        task = dag.get_task("extract_data")
        assert task is not None
        assert task.type == TaskType.EXTRACT

    def test_get_nonexistent(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        dag = PipelineDAG(config)
        assert dag.get_task("nonexistent") is None

    def test_render_ascii(self, sample_config_yaml):
        config = PipelineConfig(**sample_config_yaml)
        dag = PipelineDAG(config)
        rendered = dag.render_ascii()
        assert "extract_data" in rendered
        assert "clean_data" in rendered


class TestPipelineEngine:
    """Tests for PipelineEngine."""

    def test_load_from_file(self, yaml_config_file):
        engine = PipelineEngine(yaml_config_file)
        assert engine.config.name == "test_pipeline"
        Path(yaml_config_file).unlink(missing_ok=True)

    def test_build_dag(self, yaml_config_file):
        engine = PipelineEngine(yaml_config_file)
        dag = engine.build_dag()
        assert isinstance(dag, PipelineDAG)
        Path(yaml_config_file).unlink(missing_ok=True)

    def test_execute_empty_pipeline(self):
        """Execute a minimal pipeline with no data."""
        config = PipelineConfig(
            name="empty_test",
            data_sources=[],
            tasks=[TaskNode(id="noop", name="Noop", type=TaskType.EXTRACT)],
        )
        # Write config to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config.model_dump(), f)
            config_path = f.name

        engine = PipelineEngine(config_path)
        result = engine.execute()
        assert result.status in (ExecutionStatus.SUCCESS, ExecutionStatus.FAILED)
        Path(config_path).unlink(missing_ok=True)


class TestPipelineOrchestrator:
    """Tests for PipelineOrchestrator."""

    def setup_method(self):
        self.orchestrator = PipelineOrchestrator()

    def test_list_empty(self):
        assert self.orchestrator.list_pipelines() == []

    def test_register_and_list(self, yaml_config_file):
        self.orchestrator.register("test", yaml_config_file)
        assert "test" in self.orchestrator.list_pipelines()
        Path(yaml_config_file).unlink(missing_ok=True)

    def test_run_unregistered(self):
        with pytest.raises(KeyError):
            self.orchestrator.run("nonexistent")


# ─── Integration Tests ─────────────────────────────────────────────────

class TestEndToEnd:
    """End-to-end pipeline execution tests."""

    def test_csv_transform_filter_flow(self):
        """Test a complete CSV → transform → filter → CSV flow."""
        import pandas as pd
        import tempfile

        # Create test input CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            f.write("name,age\n Alice ,25\nBob,15\n Charlie ,30\n")
            input_path = f.name

        output_path = tempfile.mktemp(suffix=".csv")

        config_data = {
            "name": "e2e_test",
            "data_sources": [
                {"name": "source", "type": "csv", "path": input_path}
            ],
            "tasks": [
                {"id": "load", "name": "Load", "type": "extract", "input_source": "source"},
                {
                    "id": "clean",
                    "name": "Clean",
                    "type": "transform",
                    "depends_on": ["load"],
                    "transforms": [
                        {"column": "name", "operation": "strip"},
                    ],
                },
                {
                    "id": "filter",
                    "name": "Filter",
                    "type": "filter",
                    "depends_on": ["clean"],
                    "filters": [{"column": "age", "operator": "gte", "value": 18}],
                },
            ],
            "data_sinks": [
                {"name": "result", "type": "csv", "path": output_path}
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name

        engine = PipelineEngine(config_path)
        result = engine.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.tasks_executed == 3

        # Verify output
        output_df = pd.read_csv(output_path)
        assert len(output_df) == 2
        assert "Alice" in output_df["name"].values
        assert "Charlie" in output_df["name"].values

        # Cleanup
        Path(input_path).unlink(missing_ok=True)
        Path(output_path).unlink(missing_ok=True)
        Path(config_path).unlink(missing_ok=True)
