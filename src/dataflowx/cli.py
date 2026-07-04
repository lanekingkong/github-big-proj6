"""
DataFlowX CLI — Command-line interface for pipeline management.
"""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
@click.version_option(version="1.0.0", prog_name="DataFlowX")
def cli():
    """DataFlowX: Intelligent Data Pipeline Orchestration Engine."""
    pass


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
def run(config_file: str):
    """Run a pipeline from a YAML/JSON configuration file."""
    from dataflowx.core.engine import PipelineEngine

    engine = PipelineEngine(config_file)
    console.print(f"[bold green]Starting pipeline:[/bold green] {engine.config.name}")
    result = engine.execute()
    console.print(f"[bold green]Pipeline completed.[/bold green] Status: {result.status}")
    console.print(f"  Tasks executed: {result.tasks_executed}")
    console.print(f"  Rows processed: {result.rows_processed:,}")
    console.print(f"  Duration: {result.duration_seconds:.2f}s")


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
def validate(config_file: str):
    """Validate a pipeline configuration without executing it."""
    from dataflowx.core.models import PipelineConfig

    cfg = PipelineConfig.from_file(config_file)
    errors = cfg.validate()
    if errors:
        console.print("[bold red]Validation errors:[/bold red]")
        for e in errors:
            console.print(f"  [red]✗[/red] {e}")
    else:
        console.print(f"[bold green]✓[/bold green] Configuration valid.")
        console.print(f"  Pipeline: {cfg.name}")
        console.print(f"  Tasks: {len(cfg.tasks)}")
        console.print(f"  Data sources: {len(cfg.data_sources)}")
        console.print(f"  Data sinks: {len(cfg.data_sinks)}")


@cli.command()
def list():
    """List all registered pipeline tasks and connectors."""
    from dataflowx.core.registry import TaskRegistry

    registry = TaskRegistry()

    # Tasks
    table = Table(title="Available Tasks")
    table.add_column("Name", style="cyan")
    table.add_column("Category", style="green")
    table.add_column("Description")

    for task in registry.list_tasks():
        table.add_row(task["name"], task["category"], task["description"])

    console.print(table)

    # Connectors
    conn_table = Table(title="Available Connectors")
    conn_table.add_column("Name", style="cyan")
    conn_table.add_column("Type", style="yellow")
    conn_table.add_column("Description")

    for conn in registry.list_connectors():
        conn_table.add_row(conn["name"], conn["type"], conn["description"])

    console.print(conn_table)


@cli.command()
@click.argument("config_file", type=click.Path())
def visualize(config_file: str):
    """Generate a visual DAG representation of the pipeline."""
    from dataflowx.core.engine import PipelineEngine

    engine = PipelineEngine(config_file)
    dag = engine.build_dag()
    console.print(f"[bold]Pipeline DAG: {engine.config.name}[/bold]")
    console.print(dag.render_ascii())


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--interval", default=60, help="Polling interval in seconds.")
def watch(config_file: str, interval: int):
    """Watch a pipeline configuration and auto-reload on changes."""
    import time
    from dataflowx.core.engine import PipelineEngine

    console.print(f"[bold yellow]Watching {config_file} (interval: {interval}s)...[/bold yellow]")
    last_mtime = None
    try:
        while True:
            import os
            mtime = os.path.getmtime(config_file)
            if last_mtime is None:
                last_mtime = mtime
            elif mtime != last_mtime:
                console.print(f"[bold green]Config changed, re-validating...[/bold green]")
                engine = PipelineEngine(config_file)
                console.print(f"  Pipeline '{engine.config.name}' loaded successfully.")
                last_mtime = mtime
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[bold]Watch stopped.[/bold]")


if __name__ == "__main__":
    cli()
