"""
DataFlowX Pipeline Engine — Executes pipeline DAGs with intelligent optimization.

Core execution engine that loads config, builds DAG, resolves dependencies,
executes tasks in topological order, and handles retry/error recovery.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, Optional

from dataflowx.core.models import (
    PipelineConfig,
    ExecutionResult,
    ExecutionStatus,
    TaskNode,
    TaskType,
)
from dataflowx.core.registry import TaskRegistry


class PipelineDAG:
    """Represents the pipeline as a directed acyclic graph."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._task_map: Dict[str, TaskNode] = {t.id: t for t in config.tasks}

    def get_task(self, task_id: str) -> Optional[TaskNode]:
        return self._task_map.get(task_id)

    def get_dependencies(self, task_id: str) -> list[str]:
        task = self._task_map.get(task_id)
        return task.depends_on if task else []

    def get_dependents(self, task_id: str) -> list[str]:
        return [t.id for t in self.config.tasks if task_id in t.depends_on]

    def render_ascii(self) -> str:
        """Render a simple ASCII representation of the DAG."""
        lines = []
        order = self.config.topological_order()
        for task_id in order:
            task = self._task_map[task_id]
            deps = task.depends_on
            prefix = "  " + ("├─ " if deps else "── ") 
            lines.append(f"{prefix}{task_id} [{task.type.value}]")
            for dep in deps:
                lines.append(f"  │   └─ depends on: {dep}")
        return "\n".join(lines)


class PipelineEngine:
    """Main pipeline execution engine."""

    def __init__(self, config_path: str):
        self.config = PipelineConfig.from_file(config_path)
        self.dag = PipelineDAG(self.config)
        self.registry = TaskRegistry()
        self._data_cache: Dict[str, Any] = {}

    def execute(self) -> ExecutionResult:
        """Execute the full pipeline and return results."""
        started_at = datetime.now()
        tasks_executed = 0
        tasks_failed = 0
        rows_processed = 0
        task_results: Dict[str, Any] = {}
        errors: list[str] = []

        # Load all data sources
        for source in self.config.data_sources:
            try:
                self._data_cache[source.name] = self.registry.load_source(source)
            except Exception as e:
                errors.append(f"Failed to load source '{source.name}': {e}")

        if errors:
            return ExecutionResult(
                pipeline_name=self.config.name,
                status=ExecutionStatus.FAILED,
                errors=errors,
                started_at=started_at,
                completed_at=datetime.now(),
                duration_seconds=(datetime.now() - started_at).total_seconds(),
            )

        # Execute tasks in topological order
        order = self.config.topological_order()
        for task_id in order:
            task = self.dag.get_task(task_id)
            if not task:
                continue

            start_t = time.time()
            try:
                input_data = self._data_cache.get(task.input_source or "")
                result = self._execute_task(task, input_data)
                tasks_executed += 1
                task_results[task_id] = {
                    "status": "success",
                    "duration": time.time() - start_t,
                    "rows": len(result) if hasattr(result, "__len__") else 0,
                }
                # Store result for downstream tasks
                self._data_cache[task_id] = result
            except Exception as exc:
                if task.retry_count > 0:
                    for retry in range(task.retry_count):
                        try:
                            input_data = self._data_cache.get(task.input_source or "")
                            result = self._execute_task(task, input_data)
                            tasks_executed += 1
                            task_results[task_id] = {
                                "status": "success",
                                "duration": time.time() - start_t,
                                "retries": retry + 1,
                            }
                            self._data_cache[task_id] = result
                            break
                        except Exception:
                            if retry == task.retry_count - 1:
                                raise
                            time.sleep(1)
                else:
                    tasks_failed += 1
                    err_msg = f"Task '{task_id}' failed: {exc}"
                    task_results[task_id] = {"status": "failed", "error": str(exc)}
                    errors.append(err_msg)

        # Write to sinks
        for sink in self.config.data_sinks:
            try:
                # Use the last task's output as the sink input
                last_task_id = order[-1] if order else None
                output_data = self._data_cache.get(last_task_id)
                if output_data is not None:
                    self.registry.write_sink(sink, output_data)
            except Exception as e:
                errors.append(f"Failed to write sink '{sink.name}': {e}")

        completed_at = datetime.now()

        status = ExecutionStatus.FAILED if tasks_executed == 0 else (
            ExecutionStatus.PARTIAL if tasks_failed > 0 else ExecutionStatus.SUCCESS
        )

        return ExecutionResult(
            pipeline_name=self.config.name,
            status=status,
            tasks_executed=tasks_executed,
            tasks_failed=tasks_failed,
            rows_processed=rows_processed,
            duration_seconds=(completed_at - started_at).total_seconds(),
            started_at=started_at,
            completed_at=completed_at,
            task_results=task_results,
            errors=errors,
        )

    def build_dag(self) -> PipelineDAG:
        """Return the pipeline DAG for visualization."""
        return self.dag

    def _execute_task(self, task: TaskNode, input_data: Any) -> Any:
        """Execute a single task based on its type."""
        if task.type == TaskType.EXTRACT:
            return self.registry.execute_extract(task, input_data)
        elif task.type == TaskType.TRANSFORM:
            return self.registry.execute_transform(task, input_data)
        elif task.type == TaskType.FILTER:
            return self.registry.execute_filter(task, input_data)
        elif task.type == TaskType.AGGREGATE:
            return self.registry.execute_aggregate(task, input_data)
        elif task.type == TaskType.JOIN:
            right_data = self._data_cache.get(task.join.right_source if task.join else "")
            return self.registry.execute_join(task, input_data, right_data)
        elif task.type == TaskType.ENRICH:
            return self.registry.execute_enrich(task, input_data)
        elif task.type == TaskType.VALIDATE:
            return self.registry.execute_validate(task, input_data)
        elif task.type == TaskType.CUSTOM:
            return self.registry.execute_custom(task, input_data)
        elif task.type == TaskType.LOAD:
            return self.registry.execute_load(task, input_data)
        else:
            raise ValueError(f"Unknown task type: {task.type}")


class PipelineOrchestrator:
    """Orchestrates multiple pipelines with scheduling and coordination."""

    def __init__(self):
        self._pipelines: Dict[str, PipelineEngine] = {}

    def register(self, name: str, config_path: str):
        """Register a pipeline by name."""
        self._pipelines[name] = PipelineEngine(config_path)

    def run_all(self) -> Dict[str, ExecutionResult]:
        """Execute all registered pipelines."""
        results = {}
        for name, engine in self._pipelines.items():
            results[name] = engine.execute()
        return results

    def run(self, name: str) -> ExecutionResult:
        """Execute a specific pipeline by name."""
        if name not in self._pipelines:
            raise KeyError(f"Pipeline '{name}' not registered.")
        return self._pipelines[name].execute()

    def list_pipelines(self) -> list[str]:
        return list(self._pipelines.keys())
