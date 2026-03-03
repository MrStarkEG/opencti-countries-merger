"""CLI entry point for the OpenCTI Country Merger."""

import asyncio
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from opencti_country_merger.config import Settings
from opencti_country_merger.display.tables import (
    display_junk,
    display_junk_results,
    display_plan,
    display_results,
    display_unresolved,
)
from opencti_country_merger.es.client import ESClient
from opencti_country_merger.services.country_mapper import CountryMapper
from opencti_country_merger.services.discovery import DiscoveryService
from opencti_country_merger.services.merger import MergeResult, MergerService
from opencti_country_merger.services.planner import PlannerService

app = typer.Typer(
    name="opencti-country-merger",
    help="Merge duplicate country entities in OpenCTI Elasticsearch.",
)
console = Console()


async def _run_merge(
    settings: Settings,
    threshold: int,
    force: bool,
) -> None:
    """Async implementation of the merge pipeline."""
    # 1. Connect to ES
    console.print("[bold]Connecting to Elasticsearch...[/bold]")
    client = ESClient(settings)
    try:
        health = await client.health_check()
        console.print(
            f"  Cluster: [green]{health.get('cluster_name', '?')}[/green]  "
            f"Status: [green]{health.get('status', '?')}[/green]"
        )

        # 2. Discover countries
        console.print("\n[bold]Discovering country entities...[/bold]")
        discovery = DiscoveryService(client)
        disc_result = await discovery.discover()
        console.print(
            f"  Strategy: entity_type=[cyan]{disc_result.entity_type}[/cyan]"
            + (
                f" + location_type=[cyan]{disc_result.location_filter}[/cyan]"
                if disc_result.location_filter
                else ""
            )
        )
        console.print(f"  Found: [bold]{disc_result.count}[/bold] entities")
        if disc_result.sample_names:
            console.print(f"  Samples: {', '.join(disc_result.sample_names)}")

        # 3. Fetch all country entities
        console.print("\n[bold]Fetching all country entities...[/bold]")
        entities = await discovery.fetch_all_countries()
        console.print(f"  Fetched [bold]{len(entities)}[/bold] entities")

        # 4. Build merge plan
        console.print("\n[bold]Building merge plan...[/bold]")
        mapper = CountryMapper(fuzzy_threshold=threshold)
        planner = PlannerService(client, mapper)
        plan = await planner.build_plan(entities)

        # 5. Display plan
        display_plan(plan)
        display_junk(plan.junk)
        display_unresolved(plan.unresolved)

        if not plan.clusters and not plan.junk:
            console.print("\n[green]No duplicates or junk found. Nothing to do.[/green]")
            return

        # 6. Confirm
        mode_label = "[yellow]DRY RUN[/yellow]" if settings.merge_dry_run else "[red]LIVE[/red]"
        console.print(f"\nMode: {mode_label}")
        if not force and not settings.merge_dry_run:
            confirmed = typer.confirm("Proceed with merge and junk deletion?")
            if not confirmed:
                console.print("Aborted.")
                raise typer.Exit(code=1)

        merger = MergerService(client, dry_run=settings.merge_dry_run)

        # 7. Delete junk entities
        if plan.junk:
            console.print("\n[bold]Deleting junk entities...[/bold]")
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                console=console,
            ) as progress:
                task = progress.add_task("Deleting junk...", total=len(plan.junk))
                junk_results = await merger.delete_junk_entities(plan.junk, progress, task)
            display_junk_results(junk_results)

        # 8. Execute merges
        if plan.clusters:
            merge_results: list[MergeResult] = []
            total_sources = sum(len(c.source_entities) for c in plan.clusters)
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                console=console,
            ) as progress:
                task = progress.add_task("Merging...", total=total_sources)
                for cluster in plan.clusters:
                    progress.update(
                        task,
                        description=f"Merging {cluster.iso_code}...",
                    )
                    result = await merger.merge_cluster(cluster, progress, task)
                    merge_results.append(result)

            # 9. Display results
            display_results(merge_results)

            if any(r.errors for r in merge_results):
                console.print("\n[red]Some merges had errors. Check the output above.[/red]")
                raise typer.Exit(code=1)

        console.print("\n[green]Done.[/green]")
    finally:
        await client.close()


@app.command()
def merge(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Count affected docs without writing."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Skip confirmation prompt."),
    ] = False,
    threshold: Annotated[
        int,
        typer.Option("--threshold", help="Fuzzy-match threshold (0-100)."),
    ] = 80,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Verbose output."),
    ] = False,
) -> None:
    """Discover, plan, and merge duplicate country entities."""
    settings = Settings()
    if dry_run:
        settings.merge_dry_run = True
    asyncio.run(_run_merge(settings, threshold, force))


if __name__ == "__main__":
    app()
