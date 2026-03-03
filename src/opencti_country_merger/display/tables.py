"""Rich console output for plan preview and merge results."""

from rich.console import Console
from rich.table import Table

from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.planner import MergePlan
from opencti_country_merger.services.merger import MergeResult, JunkDeleteResult

console = Console()


def display_plan(plan: MergePlan) -> None:
    """Show a Rich table summarising the merge plan."""
    console.print()
    console.print(
        f"[bold]Merge Plan[/bold]: {plan.total_entities} entities discovered, "
        f"{len(plan.clusters)} clusters with duplicates, "
        f"{plan.total_merges} merges required, "
        f"{len(plan.junk)} junk entities to delete"
    )
    console.print()

    table = Table(title="Merge Clusters", show_lines=True)
    table.add_column("ISO", style="bold cyan", width=5)
    table.add_column("New Name", style="bold green")
    table.add_column("Target (current)", style="green")
    table.add_column("Target Rels", justify="right")
    table.add_column("Sources to Merge", style="yellow")
    table.add_column("Source Rels", justify="right")

    for cluster in plan.clusters:
        source_names = "\n".join(
            e.name for e in cluster.source_entities
        )
        source_rels = "\n".join(
            str(cluster.source_rel_counts.get(e.internal_id, 0))
            for e in cluster.source_entities
        )
        table.add_row(
            cluster.iso_code,
            cluster.country_name,
            cluster.target_entity.name,
            str(cluster.target_rel_count),
            source_names,
            source_rels,
        )

    console.print(table)


def display_junk(entities: list[CountryEntity]) -> None:
    """Show entities flagged as junk that will be deleted."""
    if not entities:
        return

    console.print()
    table = Table(title="Junk Entities (will be deleted with relationships)", show_lines=True)
    table.add_column("Name", style="red bold")
    table.add_column("Internal ID")
    table.add_column("Index")

    for entity in entities:
        table.add_row(entity.name, entity.internal_id, entity.index)

    console.print(table)


def display_unresolved(entities: list[CountryEntity]) -> None:
    """Show entities that could not be mapped to an ISO code."""
    if not entities:
        return

    console.print()
    table = Table(title="Unresolved Entities (no action taken)", show_lines=True)
    table.add_column("Name", style="red")
    table.add_column("Internal ID")
    table.add_column("Index")

    for entity in entities:
        table.add_row(entity.name, entity.internal_id, entity.index)

    console.print(table)


def display_results(results: list[MergeResult]) -> None:
    """Show a summary table of completed merges."""
    console.print()
    table = Table(title="Merge Results", show_lines=True)
    table.add_column("ISO", style="bold cyan", width=5)
    table.add_column("Target ID")
    table.add_column("Merged", justify="right")
    table.add_column("Updated", justify="right", style="green")
    table.add_column("Archived", justify="right", style="yellow")
    table.add_column("Deleted", justify="right", style="red")
    table.add_column("Errors", justify="right")

    total_updated = 0
    total_archived = 0
    total_deleted = 0

    for r in results:
        table.add_row(
            r.iso_code,
            r.target_id[:12] + "...",
            str(r.sources_merged),
            str(r.docs_updated),
            str(r.docs_archived),
            str(r.docs_deleted),
            str(len(r.errors)) if r.errors else "-",
        )
        total_updated += r.docs_updated
        total_archived += r.docs_archived
        total_deleted += r.docs_deleted

    console.print(table)
    console.print(
        f"\n[bold]Totals[/bold]: {total_updated} updated, "
        f"{total_archived} archived, {total_deleted} deleted"
    )

    # Print actual error messages
    errors_found = [r for r in results if r.errors]
    if errors_found:
        console.print("\n[bold red]Error Details:[/bold red]")
        for r in errors_found:
            for err in r.errors:
                console.print(f"  [red]{r.iso_code}[/red] ({r.target_id[:12]}...): {err}")


def display_junk_results(results: list[JunkDeleteResult]) -> None:
    """Show summary of junk entity deletions."""
    if not results:
        return

    console.print()
    table = Table(title="Junk Deletion Results", show_lines=True)
    table.add_column("Name", style="red bold")
    table.add_column("Entity ID")
    table.add_column("Rels Deleted", justify="right", style="red")
    table.add_column("Archived", justify="center")
    table.add_column("Deleted", justify="center")
    table.add_column("Errors", justify="right")

    total_rels = 0
    for r in results:
        table.add_row(
            r.name,
            r.entity_id[:12] + "...",
            str(r.rels_deleted),
            "Y" if r.archived else "N",
            "Y" if r.deleted else "N",
            str(len(r.errors)) if r.errors else "-",
        )
        total_rels += r.rels_deleted

    console.print(table)
    console.print(f"\n[bold]Junk totals[/bold]: {total_rels} relationships deleted, {len(results)} entities removed")

    errors_found = [r for r in results if r.errors]
    if errors_found:
        console.print("\n[bold red]Junk Error Details:[/bold red]")
        for r in errors_found:
            for err in r.errors:
                console.print(f"  [red]{r.name}[/red] ({r.entity_id[:12]}...): {err}")
