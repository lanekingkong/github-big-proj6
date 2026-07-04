"""
DataFlowX: Intelligent Data Pipeline Orchestration Engine.

An AI-powered ETL, streaming, and data transformation platform with
visual DAG editor and intelligent pipeline optimization.
"""

__version__ = "1.0.0"
__author__ = "lanekingkong"
__license__ = "MIT"

from dataflowx.core.engine import PipelineEngine
from dataflowx.core.orchestrator import PipelineOrchestrator
from dataflowx.core.models import PipelineConfig, TaskNode, DataSource, DataSink

__all__ = [
    "PipelineEngine",
    "PipelineOrchestrator",
    "PipelineConfig",
    "TaskNode",
    "DataSource",
    "DataSink",
]
