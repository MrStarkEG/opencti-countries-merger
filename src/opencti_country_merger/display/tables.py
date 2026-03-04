"""Rich console output for plan preview and merge results."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.planner import MergePlan
from opencti_country_merger.services.merger import MergeResult, JunkDeleteResult
from opencti_country_merger.services.fix_names import FixNamesPlan, FixNamesResult

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


# ------------------------------------------------------------------
# fix-names display helpers
# ------------------------------------------------------------------


def display_fix_plan(plan: FixNamesPlan) -> None:
    """Show summary statistics for a fix-names plan."""
    console.print()
    console.print(
        f"[bold]Fix-Names Plan[/bold]: "
        f"{len(plan.renames)} renames, "
        f"{len(plan.alias_replacements)} alias resets, "
        f"{len(plan.creates)} countries to create "
        f"({plan.total_actions} total actions)"
    )
    if plan.warnings:
        console.print()
        for w in plan.warnings:
            console.print(f"  [yellow]WARNING:[/yellow] {w}")


def display_renames(plan: FixNamesPlan) -> None:
    """Show a table of name changes."""
    if not plan.renames:
        return
    console.print()
    table = Table(title="Renames", show_lines=True)
    table.add_column("ISO", style="bold cyan", width=5)
    table.add_column("Current Name", style="yellow")
    table.add_column("→", width=2)
    table.add_column("New Name", style="bold green")
    for r in plan.renames:
        table.add_row(r.alpha_2, r.current_name, "→", r.new_name)
    console.print(table)


def display_alias_replacements(plan: FixNamesPlan) -> None:
    """Show a table of alias replacements."""
    if not plan.alias_replacements:
        return
    console.print()
    table = Table(title="Alias Resets (replace all → [alpha-2] only)", show_lines=True)
    table.add_column("ISO", style="bold cyan", width=5)
    table.add_column("Entity Name")
    table.add_column("Current Aliases", style="yellow")
    table.add_column("→", width=2)
    table.add_column("New Aliases", style="bold green")
    for a in plan.alias_replacements:
        current = ", ".join(a.current_aliases) if a.current_aliases else "(none)"
        new = ", ".join(a.new_aliases)
        table.add_row(a.alpha_2, a.entity_name, current, "→", new)
    console.print(table)


def display_creates(plan: FixNamesPlan) -> None:
    """Show a table of countries to create."""
    if not plan.creates:
        return
    console.print()
    table = Table(title="Countries to Create", show_lines=True)
    table.add_column("ISO", style="bold cyan", width=5)
    table.add_column("Name", style="bold green")
    table.add_column("Alpha-3", width=6)
    for c in plan.creates:
        table.add_row(c.alpha_2, c.name, c.alpha_3)
    console.print(table)


def display_fix_results(result: FixNamesResult) -> None:
    """Show fix-names execution summary."""
    console.print()
    console.print("[bold]Results:[/bold]")
    console.print(
        f"  Renames:  [green]{result.renames_ok} ok[/green]"
        + (f", [red]{result.renames_failed} failed[/red]" if result.renames_failed else "")
    )
    console.print(
        f"  Aliases:  [green]{result.aliases_ok} ok[/green]"
        + (f", [red]{result.aliases_failed} failed[/red]" if result.aliases_failed else "")
    )
    console.print(
        f"  Creates:  [green]{result.creates_ok} ok[/green]"
        + (f", [red]{result.creates_failed} failed[/red]" if result.creates_failed else "")
    )
    console.print(
        f"\n[bold]Totals[/bold]: "
        f"[green]{result.total_ok} succeeded[/green], "
        f"[red]{result.total_failed} failed[/red]"
    )
    if result.errors:
        console.print("\n[bold red]Error Details:[/bold red]")
        for err in result.errors:
            console.print(f"  [red]•[/red] {err}")
