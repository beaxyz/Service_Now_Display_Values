# ServiceNow Display Value Extraction

Extracts human-readable display values from ServiceNow reference fields using Databricks Asset Bundles (DABs), Lakeflow Connect, and Lakeflow Declarative Pipelines (LDP).

## What it does

ServiceNow stores reference fields as `sys_id` pointers (e.g. `assigned_to` contains a GUID, not a name). This project:

1. **Discovers** all tables needed (base tables + reference tables + parent tables via ServiceNow inheritance)
2. **Ingests** them via Lakeflow Connect into Unity Catalog
3. **Extracts display values** by joining each reference field to its target table and pulling the human-readable column (e.g. `name`, `number`)

The result is a set of `{table}_display` materialized views where each reference field gets a corresponding `{field}_display` column with the resolved value.

## Project structure

```
Service_Now_Display_Values/
├── config.py                   # Central configuration (catalog, schema, table list, etc.)
├── databricks.yml              # Databricks Asset Bundle definition
├── src/
│   ├── __init__.py
│   ├── session.py              # Spark + WorkspaceClient (local vs. Databricks)
│   ├── ingestion.py            # IngestionManager: table discovery, pipeline updates
│   └── display.py              # DisplayValue: display column logic, reference joins
├── pipelines/
│   ├── __init__.py
│   ├── ingest_service_now.py   # Task 1: discover tables, update Lakeflow Connect pipeline
│   ├── extract_display.py      # Task 2: LDP pipeline for display value extraction
│   └── run_test.py             # Local smoke test (runs extract logic via Databricks Connect)
└── tests/
    ├── conftest.py             # Pytest config (--run-integration flag)
    ├── unit_tests.py           # Unit tests (mocked Spark/SDK, no Databricks needed)
    └── integration_test.py     # Integration tests (requires Databricks Connect)
```

## Key components

### `config.py`

Central configuration: catalog, schema, table list, columns to use for display names (`sys_col_names`), tables to exclude, Lakeflow Connect connection/pipeline names.

### `src/session.py`

`get_spark_and_client()` -- returns a `(SparkSession, WorkspaceClient)` tuple. Automatically detects whether it's running locally (uses Databricks Connect with a profile) or on Databricks (uses the runtime's SparkSession).

### `src/ingestion.py` -- `IngestionManager`

Handles table discovery and Lakeflow Connect pipeline management:

- `get_table_with_ancestors(table_list)` -- resolves ServiceNow table inheritance chains (e.g. `incident` -> `task`) by traversing `sys_db_object.super_class`
- `get_reference_tables(table_list)` -- finds reference fields and their target tables from `sys_dictionary`
- `full_table_ingestion_list(table_list)` -- combines base tables + reference tables + ancestor tables into a deduplicated list
- `update_servicenow_pipeline(table_list)` -- updates the Lakeflow Connect pipeline definition with the full table list

### `src/display.py` -- `DisplayValue`

Determines display columns and performs the reference joins:

- `get_display_value(table_list)` -- for each table, determines the display column (from `sys_dictionary` display flag), whether it's a reference field, and the target table/column
- `join_to_ref(base_df, row)` -- joins a base table to a reference table on `sys_id`, adding a `{field}_display` column with the resolved value. Handles chained references (reference field pointing to another reference).

## Pipeline flow

```
databricks bundle deploy --target dev
databricks bundle run ingest_job
```

The job runs 3 tasks in sequence:

1. **`discover_tables`** (`pipelines/ingest_service_now.py`) -- expands `config.table_list` with reference and ancestor tables, updates the Lakeflow Connect pipeline definition
2. **`ingest_servicenow_tables`** -- Lakeflow Connect ingests all discovered ServiceNow tables into `catalog.schema`
3. **`extract_display_values`** (`pipelines/extract_display.py`) -- LDP pipeline that creates:
   - `display_value_mapping_table` -- metadata table mapping each table to its display column info
   - `{table}_display` materialized views -- one per table, with `{field}_display` columns added for each reference field

## Configuration

Edit `config.py` to customize:

```python
catalog = "field_demos"
schema = "bliew_snow_test"
source_schema = "servicenow_bliew_snow"

table_list = ["incident", "change_request", "problem", ...]
tables_to_exclude = ["sys_package"]

sys_col_names = ["name", "u_name", "number"]  # display column fallback order

connection_name = "bliew_snow"
pipeline_name = "servicenow_bliew_ingestion"
```

## Testing

### Unit tests (no Databricks needed)

```bash
python -m pytest tests/unit_tests.py -v
```

Tests pure logic (table exclusion, deduplication, config validation), the `main()` flow with mocked Spark/SDK, and `DisplayValue` class methods with mocked data.

### Integration tests (requires Databricks Connect)

```bash
# Deploy and run the pipeline first
databricks bundle deploy --target dev
databricks bundle run ingest_job

# Then run integration tests
python -m pytest tests/integration_test.py -v --run-integration -s
```

Tests against real data:

- **`TestDisplayValue`** -- validates display column logic against `asmt_metric_result`
- **`TestIngestionOutput`** -- verifies all configured tables exist and have rows
- **`TestMappingTable`** -- validates `display_value_mapping_table` schema and coverage
- **`TestDisplayTables`** -- checks `{table}_display` MVs exist and have display columns
- **`TestDisplayExtractionAccuracy`** -- verifies display column counts match struct reference fields (including inherited fields from parent tables), and non-null display counts are within expected bounds

### Local smoke test

```bash
python pipelines/run_test.py
```

Runs the display extraction logic locally for the `incident` table via Databricks Connect, printing intermediate counts and column info for debugging.

## Deployment

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) with a configured profile.

```bash
# Deploy the bundle
databricks bundle deploy --target dev

# Run the full pipeline
databricks bundle run ingest_job

# Run just the display extraction (with full refresh if schema changed)
databricks bundle run extract_display_pipeline --refresh-all
```

**Important:** Always `databricks bundle deploy` before triggering a full refresh, otherwise DLT runs the previously deployed code.
