# OpenCTI Country Merger

CLI tool to deduplicate, normalize, and link geographic entities (Countries and Regions) in OpenCTI's Elasticsearch.

## Setup

```bash
# Install
uv sync

# Configure (copy and edit)
cp .env.example .env
```

Required `.env` variables:
```
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_USERNAME=elastic
ELASTICSEARCH_PASSWORD=changeme
```

## Commands

### `merge` — Deduplicate country entities

Finds duplicate countries (by ISO code), merges relationships into the best target, archives and deletes sources.

```bash
opencti-country-merger merge --dry-run     # Preview
opencti-country-merger merge               # Execute (with confirmation)
opencti-country-merger merge --force       # Skip confirmation
```

### `fix-names` — Normalize country names and aliases

Renames countries to their canonical ISO 3166-1 name, resets aliases to `[alpha-2]`, creates missing countries.

```bash
opencti-country-merger fix-names --dry-run
opencti-country-merger fix-names --force
```

### `fix-regions` — Deduplicate and normalize regions

Merges duplicate regions (e.g. `central-america` + `Central America`), renames to Title Case, sets aliases to `[M49 code]`, creates missing UN M49 regions.

```bash
opencti-country-merger fix-regions --dry-run
opencti-country-merger fix-regions --force
```

### `link-regions` — Connect countries to their UN M49 regions

Creates `located-at` relationships between each country and its sub-region + macro-region (e.g. France → Western Europe AND France → Europe).

```bash
opencti-country-merger link-regions --dry-run
opencti-country-merger link-regions --force
```

## Recommended execution order

```bash
# 1. Merge duplicate countries
opencti-country-merger merge --force

# 2. Fix country names and aliases
opencti-country-merger fix-names --force

# 3. Merge and normalize regions
opencti-country-merger fix-regions --force
# Run twice — first pass merges duplicates, second pass cleans aliases on merged entities
opencti-country-merger fix-regions --force

# 4. Link countries to regions
opencti-country-merger link-regions --force

# 5. Flush Redis cache and restart OpenCTI
```

## Docker

```bash
docker compose up -d
```

Set `MERGER_COMMAND` to any subcommand (`merge`, `fix-names`, `fix-regions`, `link-regions`). Runs on cron (default: 2 AM daily). Set `RUN_NOW=true` to execute immediately on startup.
