"""
Integration tests — requires Databricks Connect.

Run AFTER deploying the bundle and running both the ingestion job and LDP pipeline:
    databricks bundle deploy --target dev
    databricks bundle run ingest_job
    databricks bundle run extract_display_pipeline

Then:
    python -m pytest tests/integration_test.py -v --run-integration -s
"""
import warnings
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from config import catalog, schema, table_list, tables_to_exclude, sys_col_names


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    from src.session import get_spark_and_client
    s, _ = get_spark_and_client()
    return s


# ---------------------------------------------------------------------------
# DisplayValue class tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDisplayValue:
    """Integration tests for DisplayValue class — requires Databricks Connect."""

    @pytest.fixture(autouse=True)
    def setup(self, spark):
        from src.display import DisplayValue
        self.dv = DisplayValue(spark, catalog, schema, sys_col_names)

    def test_check_display_true(self):
        rows = self.dv.check_display_true("asmt_metric_result")
        assert len(rows) == 1
        assert rows[0]["name"] == "asmt_metric_result"
        assert rows[0]["element"] == "metric"
        assert rows[0]["display"] == True

    def test_get_elements(self):
        result = self.dv.get_elements("asmt_metric_result")
        assert result is None

    def test_check_if_display_is_reference(self):
        result = self.dv.check_if_display_is_reference("asmt_metric_result", "metric")
        assert result == True

    def test_return_display_value_if_reference(self):
        result = self.dv.return_display_value_if_reference("asmt_metric_result", "metric")
        assert result == {"reference_table": "asmt_metric", "display_value": "name"}

    def test_get_display_value(self):
        result = self.dv.get_display_value(["asmt_metric_result"])
        assert len(result) == 1
        assert result[0] == {
            "table": "asmt_metric_result",
            "element": "metric",
            "display_flag": True,
            "has_parent": None,
            "parent_table": None,
            "is_display_reference_field": True,
            "reference_table_for_display_value": "asmt_metric",
            "display_value": "name",
        }


# ---------------------------------------------------------------------------
# Pipeline output: ingestion tables (parametrized per table)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestIngestionOutput:
    """Validates that the ingestion job created the expected tables in the catalog."""

    @pytest.fixture(autouse=True)
    def setup(self, spark):
        self.spark = spark
        info_schema = spark.read.table(f"{catalog}.information_schema.tables")
        self.existing_tables = {
            row["table_name"]
            for row in info_schema.filter(
                info_schema.table_schema == schema
            ).select("table_name").collect()
        }

    @pytest.mark.parametrize("table", table_list)
    def test_table_exists_in_catalog(self, table):
        """Table should exist in catalog.schema."""
        assert table in self.existing_tables, \
            f"{table} not found in {catalog}.{schema}"

    @pytest.mark.parametrize("table", table_list)
    def test_table_row_count_nonzero(self, table):
        """Ingested table should have at least 1 row."""
        full_name = f"{catalog}.{schema}.{table}"
        count = self.spark.read.table(full_name).count()
        print(f"[{table}] row count: {count}")
        assert count > 0, f"{full_name} has 0 rows"

    @pytest.mark.parametrize("table", tables_to_exclude)
    def test_excluded_table_not_ingested(self, table):
        """Tables in config.tables_to_exclude should not appear as standalone tables."""
        assert table not in self.existing_tables, \
            f"Excluded table {table} should not exist in {catalog}.{schema}"


# ---------------------------------------------------------------------------
# Pipeline output: display_value_mapping_table
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestMappingTable:
    """Validates the display_value_mapping_table created by the LDP pipeline."""

    @pytest.fixture(autouse=True)
    def setup(self, spark):
        self.spark = spark
        self.mapping_table = f"{catalog}.{schema}.display_value_mapping_table"

    def test_mapping_table_exists_and_has_rows(self):
        """display_value_mapping_table should exist and contain data."""
        count = self.spark.read.table(self.mapping_table).count()
        print(f"[display_value_mapping_table] row count: {count}")
        assert count > 0, f"{self.mapping_table} has 0 rows"

    def test_mapping_table_has_expected_columns(self):
        """Schema should have the 8 expected columns."""
        df = self.spark.read.table(self.mapping_table)
        expected_cols = {
            "table", "display_flag", "element", "has_parent",
            "parent_table", "is_display_reference_field",
            "reference_table_for_display_value", "display_value",
        }
        actual_cols = set(df.columns)
        assert expected_cols == actual_cols, \
            f"Column mismatch. Expected: {expected_cols}, Got: {actual_cols}"

    def test_mapping_table_covers_all_ingested_tables(self):
        """The distinct 'table' values should cover every table in config.table_list."""
        df = self.spark.read.table(self.mapping_table)
        mapped_tables = {row["table"] for row in df.select("table").distinct().collect()}
        expected = set(table_list) - set(tables_to_exclude)
        missing = expected - mapped_tables
        assert missing == set(), f"Tables missing from mapping: {missing}"


# ---------------------------------------------------------------------------
# Pipeline output: *_display materialized views (parametrized per table)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDisplayTables:
    """Validates the per-table *_display materialized views created by the LDP pipeline."""

    @pytest.fixture(autouse=True)
    def setup(self, spark):
        self.spark = spark

    @pytest.mark.parametrize("table", table_list)
    def test_display_table_exists(self, table):
        """A {table}_display table should exist."""
        info_schema = self.spark.read.table(f"{catalog}.information_schema.tables")
        existing = {
            row["table_name"]
            for row in info_schema.filter(
                info_schema.table_schema == schema
            ).select("table_name").collect()
        }
        display_name = f"{table}_display"
        assert display_name in existing, \
            f"{display_name} not found in {catalog}.{schema}"

    @pytest.mark.parametrize("table", table_list)
    def test_display_table_has_resolved_columns(self, table):
        """Reference columns in *_display table should be resolved (non-struct)."""
        base_df = self.spark.read.table(f"{catalog}.{schema}.{table}")
        display_df = self.spark.read.table(f"{catalog}.{schema}.{table}_display")
        struct_cols_in_base = [
            f.name for f in base_df.schema.fields
            if isinstance(f.dataType, T.StructType)
        ]
        resolved = [
            c for c in struct_cols_in_base
            if c in display_df.columns
            and not isinstance(display_df.schema[c].dataType, T.StructType)
        ]
        print(f"[{table}] resolved columns ({len(resolved)}): {resolved}")
        assert len(resolved) > 0, \
            f"{catalog}.{schema}.{table}_display has no resolved reference columns"

    @pytest.mark.parametrize("table", table_list)
    def test_display_table_row_count_matches_base(self, table):
        """Row count of {table}_display should match the base table (left joins preserve rows)."""
        base_count = self.spark.read.table(f"{catalog}.{schema}.{table}").count()
        display_count = self.spark.read.table(f"{catalog}.{schema}.{table}_display").count()
        print(f"[{table}] base rows: {base_count}, display rows: {display_count}")
        assert display_count == base_count, \
            f"{table}_display has {display_count} rows but base has {base_count}"


# ---------------------------------------------------------------------------
# Display extraction validation: column counts & row-level non-null checks
# (parametrized per table)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDisplayExtractionAccuracy:
    """Validates that display column extraction is correct for each table.

    1) The number of _display columns should match the number of reference
       fields (including inherited ones from ancestor tables) that are actually
       StructType in the base table.
    2) For each struct-typed reference field, the non-null count in the display
       column should be <= the non-null count in the base reference column
       (may be less if the reference table doesn't contain all referenced sys_ids).
    """

    @pytest.fixture(autouse=True)
    def setup(self, spark):
        from src.ingestion import IngestionManager
        from src.session import get_spark_and_client
        from config import source_schema, connection_name, pipeline_name
        _, w = get_spark_and_client()
        self.spark = spark
        self.df_sys_dict = spark.read.table(f"{catalog}.{schema}.sys_dictionary")
        self.ingestion_manager = IngestionManager(
            spark, w, catalog, schema, source_schema, pipeline_name, connection_name
        )

    def _get_struct_reference_fields(self, table):
        """Return reference field names that are StructType in the base table,
        including inherited fields from ancestor tables (matches DLT pipeline logic)."""
        # Get ancestor tables (e.g. incident -> [incident, task])
        ancestor_tables = list(
            self.ingestion_manager.get_table_with_ancestors([table])[0].values()
        )

        # Get all reference fields from sys_dictionary for the table AND its ancestors
        rows = (
            self.df_sys_dict
            .filter(F.col("name").isin(ancestor_tables) & F.col("reference").isNotNull())
            .select(F.col("element").alias("sys_name"))
            .distinct()
            .collect()
        )
        all_ref_fields = [row["sys_name"] for row in rows]

        # Filter to only those that are StructType in the actual table
        base_df = self.spark.read.table(f"{catalog}.{schema}.{table}")
        struct_fields = []
        for field in all_ref_fields:
            if field in base_df.columns:
                if isinstance(base_df.schema[field].dataType, T.StructType):
                    struct_fields.append(field)
        return struct_fields

    @pytest.mark.parametrize("table", table_list)
    def test_resolved_column_count_matches_struct_reference_fields(self, table):
        """# of resolved (struct→non-struct) columns in display table == struct ref fields in base."""
        struct_ref_fields = self._get_struct_reference_fields(table)

        base_df = self.spark.read.table(f"{catalog}.{schema}.{table}")
        display_df = self.spark.read.table(f"{catalog}.{schema}.{table}_display")

        resolved_cols = [
            f for f in struct_ref_fields
            if f in display_df.columns
            and not isinstance(display_df.schema[f].dataType, T.StructType)
        ]

        print(
            f"[{table}] base table cols: {len(base_df.columns)}, "
            f"display table cols: {len(display_df.columns)}\n"
            f"  struct ref fields: {len(struct_ref_fields)}, "
            f"resolved cols: {len(resolved_cols)}\n"
            f"  ref fields: {sorted(struct_ref_fields)}\n"
            f"  resolved: {sorted(resolved_cols)}\n"
            f"  display table ALL cols: {sorted(display_df.columns)}"
        )

        assert len(resolved_cols) == len(struct_ref_fields), (
            f"{table}: expected {len(struct_ref_fields)} resolved columns "
            f"but found {len(resolved_cols)}. "
            f"Unresolved: {sorted(set(struct_ref_fields) - set(resolved_cols))}"
        )

    @pytest.mark.parametrize("table", table_list)
    def test_resolved_column_nonnull_within_base_nonnull(self, table):
        """For each struct reference field, resolved non-null count should be
        <= base non-null count (some refs may not have matching records)."""
        struct_ref_fields = self._get_struct_reference_fields(table)
        if not struct_ref_fields:
            print(f"[{table}] no struct reference fields, skipping")
            return

        base_df = self.spark.read.table(f"{catalog}.{schema}.{table}")
        display_df = self.spark.read.table(f"{catalog}.{schema}.{table}_display")

        for ref_field in struct_ref_fields:
            if ref_field not in display_df.columns:
                print(f"[{table}] {ref_field} not in display table, skipping")
                continue

            base_nonnull = base_df.filter(
                F.col(ref_field).getField("value").isNotNull()
            ).count()

            display_nonnull = display_df.filter(
                F.col(ref_field).isNotNull()
            ).count()

            match_rate = (display_nonnull / base_nonnull * 100) if base_nonnull > 0 else 100.0

            print(
                f"[{table}.{ref_field}] "
                f"base non-null: {base_nonnull}, "
                f"resolved non-null: {display_nonnull}, "
                f"match rate: {match_rate:.1f}%"
            )

            if base_nonnull > 0 and match_rate < 50:
                warnings.warn(
                    f"{table}.{ref_field}: low match rate {match_rate:.1f}% — "
                    f"only {display_nonnull}/{base_nonnull} refs resolved to display values. "
                    f"Reference table may be missing rows."
                )

            assert display_nonnull <= base_nonnull, (
                f"{table}.{ref_field}: resolved has {display_nonnull} non-null rows "
                f"but base only has {base_nonnull} non-null refs — "
                f"resolved should never exceed base"
            )
