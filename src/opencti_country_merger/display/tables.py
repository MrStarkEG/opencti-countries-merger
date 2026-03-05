"""Rich console output for plan preview and merge results."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from opencti_country_merger.services.discovery import CountryEntity
from opencti_country_merger.services.planner import MergePlan
from opencti_country_merger.services.merger import MergeResult, JunkDeleteResult
from opencti_country_merger.services.fix_names import FixNamesPlan, FixNamesResult
from opencti_country_merger.services.fix_regions import RegionPlan, RegionResult
from opencti_country_merger.services.link_regions import LinkPlan, LinkResult

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


# ------------------------------------------------------------------
# fix-regions display helpers
# ------------------------------------------------------------------


def display_region_plan(plan: RegionPlan) -> None:
    """Show summary statistics for a fix-regions plan."""
    console.print()
    console.print(
        f"[bold]Fix-Regions Plan[/bold]: "
        f"{len(plan.merge_groups)} merge groups ({plan.total_merges} merges), "
        f"{len(plan.fixes)} fixes, "
        f"{len(plan.creates)} regions to create, "
        f"{len(plan.junk)} junk to delete "
        f"({plan.total_actions} total actions)"
    )


def display_region_merges(plan: RegionPlan) -> None:
    """Show a table of duplicate region groups to merge."""
    if not plan.merge_groups:
        return
    console.print()
    table = Table(title="Region Merge Groups", show_lines=True)
    table.add_column("Canonical Name", style="bold green")
    table.add_column("Target (current)", style="green")
    table.add_column("Target Rels", justify="right")
    table.add_column("Sources to Merge", style="yellow")
    table.add_column("Source Rels", justify="right")

    for group in plan.merge_groups:
        sorted_entities = sorted(
            group.entities,
            key=lambda e: (
                -group.rel_counts.get(e.internal_id, 0),
                e.source.get("created_at", ""),
            ),
        )
        target = sorted_entities[0]
        sources = sorted_entities[1:]
        source_names = "\n".join(e.name for e in sources)
        source_rels = "\n".join(
            str(group.rel_counts.get(e.internal_id, 0)) for e in sources
        )
        table.add_row(
            group.canonical.name,
            target.name,
            str(group.rel_counts.get(target.internal_id, 0)),
            source_names,
            source_rels,
        )
    console.print(table)


def display_region_fixes(plan: RegionPlan) -> None:
    """Show a table of name/alias fixes for existing regions."""
    if not plan.fixes:
        return
    console.print()
    table = Table(title="Region Name/Alias Fixes", show_lines=True)
    table.add_column("Current Name", style="yellow")
    table.add_column("->", width=2)
    table.add_column("New Name", style="bold green")
    table.add_column("New Aliases", style="cyan")
    for fix in plan.fixes:
        aliases_str = ", ".join(fix.new_aliases) if fix.new_aliases else "(none)"
        table.add_row(fix.current_name, "->", fix.new_name, aliases_str)
    console.print(table)


def display_region_creates(plan: RegionPlan) -> None:
    """Show a table of regions to create."""
    if not plan.creates:
        return
    console.print()
    table = Table(title="Regions to Create", show_lines=True)
    table.add_column("Name", style="bold green")
    table.add_column("M49 Code", style="cyan", width=8)
    for action in plan.creates:
        table.add_row(action.name, action.m49_code or "-")
    console.print(table)


def display_region_junk(plan: RegionPlan) -> None:
    """Show a table of junk region entities to delete."""
    if not plan.junk:
        return
    console.print()
    table = Table(title="Junk Regions (0 relationships, not in reference list)", show_lines=True)
    table.add_column("Name", style="red bold")
    table.add_column("Internal ID")
    table.add_column("Index")
    for entity in plan.junk:
        table.add_row(entity.name, entity.internal_id, entity.index)
    console.print(table)


def display_region_results(result: RegionResult) -> None:
    """Show fix-regions execution summary."""
    console.print()
    console.print("[bold]Results:[/bold]")
    if result.merges_ok or result.merges_failed:
        console.print(
            f"  Merges:   [green]{result.merges_ok} ok[/green]"
            + (f", [red]{result.merges_failed} failed[/red]" if result.merges_failed else "")
        )
    if result.fixes_ok or result.fixes_failed:
        console.print(
            f"  Fixes:    [green]{result.fixes_ok} ok[/green]"
            + (f", [red]{result.fixes_failed} failed[/red]" if result.fixes_failed else "")
        )
    if result.creates_ok or result.creates_failed:
        console.print(
            f"  Creates:  [green]{result.creates_ok} ok[/green]"
            + (f", [red]{result.creates_failed} failed[/red]" if result.creates_failed else "")
        )
    if result.junk_ok or result.junk_failed:
        console.print(
            f"  Junk:     [green]{result.junk_ok} ok[/green]"
            + (f", [red]{result.junk_failed} failed[/red]" if result.junk_failed else "")
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

    # Show merge detail table if we have merge results
    if result.merge_results:
        console.print()
        table = Table(title="Merge Details", show_lines=True)
        table.add_column("Region", style="bold cyan")
        table.add_column("Target ID")
        table.add_column("Merged", justify="right")
        table.add_column("Updated", justify="right", style="green")
        table.add_column("Archived", justify="right", style="yellow")
        table.add_column("Deleted", justify="right", style="red")
        table.add_column("Errors", justify="right")
        for r in result.merge_results:
            table.add_row(
                r.iso_code,
                r.target_id[:12] + "...",
                str(r.sources_merged),
                str(r.docs_updated),
                str(r.docs_archived),
                str(r.docs_deleted),
                str(len(r.errors)) if r.errors else "-",
            )
        console.print(table)


# ------------------------------------------------------------------
# link-regions display helpers
# ------------------------------------------------------------------


def display_link_plan(plan: LinkPlan) -> None:
    """Show summary statistics for a link-regions plan."""
    console.print()
    console.print(
        f"[bold]Link-Regions Plan[/bold]: "
        f"{len(plan.to_create)} links to create, "
        f"{plan.already_linked} already linked, "
        f"{len(plan.unmatched_countries)} countries without a region mapping"
    )


def display_link_actions(plan: LinkPlan) -> None:
    """Show a table of country → region links to create."""
    if not plan.to_create:
        return
    console.print()

    # Group by region for cleaner display
    by_region: dict[str, list[str]] = {}
    for action in plan.to_create:
        by_region.setdefault(action.region.name, []).append(action.country.name)

    table = Table(title="Country → Region Links to Create", show_lines=True)
    table.add_column("Region", style="bold cyan")
    table.add_column("M49", width=5)
    table.add_column("Countries", style="green")
    table.add_column("#", justify="right", width=4)

    for action in plan.to_create:
        region_name = action.region.name
        if region_name in by_region:
            countries = sorted(by_region.pop(region_name))
            table.add_row(
                region_name,
                action.region_m49,
                ", ".join(countries),
                str(len(countries)),
            )

    console.print(table)


def display_link_unmatched(plan: LinkPlan) -> None:
    """Show countries that couldn't be matched to a region."""
    if not plan.unmatched_countries:
        return
    console.print()
    table = Table(title="Unmatched Countries (no region mapping)", show_lines=True)
    table.add_column("Name", style="yellow")
    table.add_column("Internal ID")
    for c in sorted(plan.unmatched_countries, key=lambda x: x.name):
        table.add_row(c.name, c.internal_id[:16] + "...")
    console.print(table)


def display_link_results(result: LinkResult) -> None:
    """Show link-regions execution summary."""
    console.print()
    console.print("[bold]Results:[/bold]")
    console.print(
        f"  Created: [green]{result.created} ok[/green]"
        + (f", [red]{result.failed} failed[/red]" if result.failed else "")
    )
    if result.errors:
        console.print("\n[bold red]Error Details:[/bold red]")
        for err in result.errors:
            console.print(f"  [red]•[/red] {err}")
