"""
Tests for Service Now ingestion.

Layer 1: Pure logic (no Spark, no mocks)
Layer 2: main() end-to-end with mocked Spark/SDK (tests pipelines/ingest_service_now.py)
"""
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# 1. Table exclusion filtering (on expanded list from full_table_ingestion_list)
# ---------------------------------------------------------------------------

class TestTableExclusion:

    def test_excludes_ref_table_and_count_is_correct(self):
        """After exclusion, the table count should drop by exactly the number excluded."""
        all_tables = [
            "incident", "task", "sys_user",            # base
            "sys_package", "cmdb_ci", "core_company",  # ref/parent
        ]
        tables_to_exclude = ["sys_package"]
        result = [t for t in all_tables if t not in tables_to_exclude]
        assert "sys_package" not in result
        assert len(result) == len(all_tables) - len(tables_to_exclude)

    def test_excludes_multiple_and_keeps_the_rest(self):
        all_tables = ["incident", "task", "sys_package", "cmdb_ci", "core_company"]
        tables_to_exclude = ["sys_package", "cmdb_ci"]
        result = [t for t in all_tables if t not in tables_to_exclude]
        assert result == ["incident", "task", "core_company"]
        assert len(result) == 3

    def test_no_exclusions_keeps_full_expanded_list(self):
        all_tables = ["incident", "task", "cmdb_ci", "core_company"]
        result = [t for t in all_tables if t not in []]
        assert result == all_tables
        assert len(result) == 4


# ---------------------------------------------------------------------------
# 2. New tables detection (set diff: expanded list - existing pipeline tables)
# ---------------------------------------------------------------------------

class TestNewTablesDetection:

    def test_detects_new_base_and_ref_tables_with_correct_count(self):
        all_tables = {"incident", "task", "sys_user", "cmdb_ci", "core_company"}
        existing = {"incident", "task"}
        new = all_tables - existing
        assert new == {"sys_user", "cmdb_ci", "core_company"}
        assert len(new) == 3

    def test_no_new_tables_when_all_exist(self):
        all_tables = {"incident", "task", "cmdb_ci"}
        existing = {"incident", "task", "cmdb_ci", "sys_user"}
        new = all_tables - existing
        assert new == set()
        assert len(new) == 0

    def test_adding_new_base_table_brings_its_refs(self):
        """Adding 'problem' to config expands to include 'cmdb_ci' as a ref. Both are new."""
        all_tables = {"incident", "task", "problem", "cmdb_ci"}
        existing = {"incident", "task"}
        new = all_tables - existing
        assert "problem" in new
        assert "cmdb_ci" in new
        assert len(new) == 2


# ---------------------------------------------------------------------------
# 3. Deduplication across base + ref + parent lists
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_overlapping_ref_and_base_deduped_with_correct_count(self):
        base = ["incident", "task", "sys_user"]
        refs = ["task", "cmdb_ci", "core_company"]  # "task" overlaps
        combined = list(dict.fromkeys(base + refs))
        assert len(combined) == 5
        assert combined.count("task") == 1

    def test_overlap_across_all_three_lists(self):
        base = ["incident", "task"]
        refs = ["cmdb_ci", "core_company"]
        parents = ["cmdb_ci", "sys_metadata"]  # "cmdb_ci" overlaps with refs
        combined = list(dict.fromkeys(base + refs + parents))
        assert len(combined) == 5
        assert combined.count("cmdb_ci") == 1

    def test_base_tables_always_come_first(self):
        base = ["incident", "task"]
        refs = ["sys_user_group"]
        parents = ["sys_metadata"]
        combined = list(dict.fromkeys(base + refs + parents))
        assert combined == ["incident", "task", "sys_user_group", "sys_metadata"]
        assert combined.index("incident") < combined.index("sys_user_group")


# ---------------------------------------------------------------------------
# 4. Config validation
# ---------------------------------------------------------------------------

class TestConfig:

    def test_table_list_not_empty_and_no_duplicates(self):
        from config import table_list
        assert len(table_list) > 0, "table_list must have at least one table"
        assert len(table_list) == len(set(table_list)), \
            f"Duplicates in config: {[t for t in table_list if table_list.count(t) > 1]}"

    def test_tables_to_exclude_not_in_base_table_list(self):
        """Excluded tables are meant for ref/parent tables (e.g. sys_package),
        not base tables. Having a table in both is contradictory."""
        from config import table_list, tables_to_exclude
        overlap = set(table_list) & set(tables_to_exclude)
        assert overlap == set(), \
            f"Contradictory: in both table_list and tables_to_exclude: {overlap}"

    def test_required_config_values_defined(self):
        from config import catalog, schema, source_schema, connection_name, pipeline_name
        for name, val in [("catalog", catalog), ("schema", schema),
                          ("source_schema", source_schema),
                          ("connection_name", connection_name),
                          ("pipeline_name", pipeline_name)]:
            assert isinstance(val, str) and len(val) > 0, f"{name} must be a non-empty string"


# ===========================================================================
# Layer 2: Test main() end-to-end (mocked Spark & SDK)
# Tests the actual flow in pipelines/ingest_service_now.py:
#   full_table_ingestion_list -> exclude -> get_existing -> update pipeline
# ===========================================================================

# ---------------------------------------------------------------------------
# Pre-mock pyspark & databricks SDK in sys.modules to prevent numpy segfault
# on macOS when the @patch decorators trigger the import chain:
#   pipelines.ingest_service_now -> src.session -> pyspark -> numpy -> SEGFAULT
# ---------------------------------------------------------------------------
import sys as _sys

_pyspark_mods = [
    'pyspark', 'pyspark.sql', 'pyspark.sql.functions', 'pyspark.sql.types',
    'pyspark.sql.session', 'pyspark.sql.dataframe', 'pyspark.sql.column',
    'pyspark.errors',
]
_databricks_mods = [
    'databricks.sdk', 'databricks.sdk.service', 'databricks.sdk.service.pipelines',
    'databricks.connect', 'databricks.connect.session',
]
for _mod in _pyspark_mods + _databricks_mods:
    _sys.modules.setdefault(_mod, MagicMock())


class TestMainFlow:
    """Tests pipelines/ingest_service_now.main() with mocked dependencies.
    
    Verifies that:
      - full_table_ingestion_list output is filtered by tables_to_exclude
      - update_servicenow_pipeline is called with full list when pipeline has tables
      - update_servicenow_pipeline is NOT called when pipeline is empty
    """

    @patch("pipelines.ingest_service_now.tables_to_exclude", ["sys_package"])
    @patch("pipelines.ingest_service_now.table_list", ["incident", "task"])
    @patch("pipelines.ingest_service_now.IngestionManager")
    @patch("pipelines.ingest_service_now.get_spark_and_client")
    def test_all_tables_minus_exclusions_sent_to_update(
        self, mock_get_spark, MockIngestionManager
    ):
        """main() should send all tables (minus exclusions) to update_servicenow_pipeline."""
        mock_get_spark.return_value = (MagicMock(), MagicMock())
        mock_mgr = MockIngestionManager.return_value
        # full_table_ingestion_list returns base + ref/parent (includes sys_package)
        mock_mgr.full_table_ingestion_list.return_value = [
            "incident", "task", "sys_user", "sys_package", "cmdb_ci", "core_company"
        ]
        # Pipeline already has tables -> triggers update
        mock_mgr.get_existing_pipeline_tables.return_value = {"incident", "task"}

        from pipelines.ingest_service_now import main
        main()

        # update is called with ALL tables minus exclusions (sys_package removed)
        call_args = mock_mgr.update_servicenow_pipeline.call_args
        tables_passed = call_args[1]["table_list"]  # keyword arg
        assert "sys_package" not in tables_passed     # excluded
        assert "incident" in tables_passed            # kept (full list, not just new)
        assert "task" in tables_passed
        assert "sys_user" in tables_passed
        assert "cmdb_ci" in tables_passed
        assert "core_company" in tables_passed
        assert len(tables_passed) == 5

    @patch("pipelines.ingest_service_now.tables_to_exclude", ["sys_package"])
    @patch("pipelines.ingest_service_now.table_list", ["incident", "task"])
    @patch("pipelines.ingest_service_now.IngestionManager")
    @patch("pipelines.ingest_service_now.get_spark_and_client")
    def test_no_update_when_pipeline_empty(
        self, mock_get_spark, MockIngestionManager
    ):
        """main() should NOT call update when pipeline has no existing tables (empty set)."""
        mock_get_spark.return_value = (MagicMock(), MagicMock())
        mock_mgr = MockIngestionManager.return_value
        mock_mgr.full_table_ingestion_list.return_value = ["incident", "task"]
        mock_mgr.get_existing_pipeline_tables.return_value = set()  # empty -> falsy

        from pipelines.ingest_service_now import main
        main()

        mock_mgr.update_servicenow_pipeline.assert_not_called()

    @patch("pipelines.ingest_service_now.tables_to_exclude", [])
    @patch("pipelines.ingest_service_now.table_list", ["incident", "task"])
    @patch("pipelines.ingest_service_now.IngestionManager")
    @patch("pipelines.ingest_service_now.get_spark_and_client")
    def test_no_exclusions_sends_full_list(
        self, mock_get_spark, MockIngestionManager
    ):
        """When tables_to_exclude is empty, all tables should be sent."""
        mock_get_spark.return_value = (MagicMock(), MagicMock())
        mock_mgr = MockIngestionManager.return_value
        mock_mgr.full_table_ingestion_list.return_value = [
            "incident", "task", "sys_package", "cmdb_ci"
        ]
        mock_mgr.get_existing_pipeline_tables.return_value = {"incident"}

        from pipelines.ingest_service_now import main
        main()

        call_args = mock_mgr.update_servicenow_pipeline.call_args
        tables_passed = call_args[1]["table_list"]
        assert "sys_package" in tables_passed  # NOT excluded this time
        assert len(tables_passed) == 4


# ===========================================================================
# Layer 3: Unit tests for DisplayValue class (mocked Spark — no Databricks needed)
# NOTE: pyspark imports are deferred to avoid numpy segfault on macOS during collection
# ===========================================================================


# ---------------------------------------------------------------------------
# Helper: create a DisplayValue without hitting real Spark
# ---------------------------------------------------------------------------
def make_display_value(sys_col_names=None):
    """Build a DisplayValue with a mocked spark session."""
    if sys_col_names is None:
        sys_col_names = ["name", "u_name", "number"]

    mock_spark = MagicMock()
    # __init__ calls spark.read.table() twice — return mock DataFrames
    mock_spark.read.table.return_value = MagicMock()

    from src.display import DisplayValue
    dv = DisplayValue(mock_spark, "my_catalog", "my_schema", sys_col_names)
    return dv


# ---------------------------------------------------------------------------
# 1. check_display_true
# ---------------------------------------------------------------------------
class TestCheckDisplayTrue:

    def test_returns_rows_from_filtered_sys_dict(self):
        dv = make_display_value()
        fake_row = MagicMock()
        fake_row.__getitem__ = lambda self, key: {"name": "incident", "element": "number", "display": True}[key]

        # Make the chain: df_sys_dict.filter(...).select(...).collect()
        dv.df_sys_dict.filter.return_value.select.return_value.collect.return_value = [fake_row]

        result = dv.check_display_true("incident")
        assert len(result) == 1

    def test_returns_empty_when_no_display_flag(self):
        dv = make_display_value()
        dv.df_sys_dict.filter.return_value.select.return_value.collect.return_value = []

        result = dv.check_display_true("some_table")
        assert result == []
        assert len(result) == 0

    def test_calls_filter_with_correct_table(self):
        dv = make_display_value()
        dv.df_sys_dict.filter.return_value.select.return_value.collect.return_value = []

        dv.check_display_true("incident")
        # Verify filter was called (i.e. the method used self.df_sys_dict)
        dv.df_sys_dict.filter.assert_called_once()


# ---------------------------------------------------------------------------
# 2. get_elements
# ---------------------------------------------------------------------------
class TestGetElements:

    def test_returns_matching_column_name(self):
        dv = make_display_value(sys_col_names=["name", "u_name", "number"])

        # Simulate: df_sys_dict.filter(...).select("element").distinct().collect()
        # Returns rows where element values are "name", "category", "priority"
        mock_rows = [MagicMock(), MagicMock(), MagicMock()]
        mock_rows[0].__getitem__ = lambda self, idx: "category"
        mock_rows[1].__getitem__ = lambda self, idx: "name"
        mock_rows[2].__getitem__ = lambda self, idx: "priority"

        dv.df_sys_dict.filter.return_value.select.return_value.distinct.return_value.collect.return_value = mock_rows

        result = dv.get_elements("incident")
        assert result == "name"  # first match in sys_col_names order

    def test_returns_none_when_no_match(self):
        dv = make_display_value(sys_col_names=["name", "u_name", "number"])

        mock_rows = [MagicMock()]
        mock_rows[0].__getitem__ = lambda self, idx: "category"

        dv.df_sys_dict.filter.return_value.select.return_value.distinct.return_value.collect.return_value = mock_rows

        result = dv.get_elements("some_table")
        assert result is None

    def test_returns_none_for_empty_elements(self):
        dv = make_display_value()
        dv.df_sys_dict.filter.return_value.select.return_value.distinct.return_value.collect.return_value = []

        result = dv.get_elements("empty_table")
        assert result is None


# ---------------------------------------------------------------------------
# 3. __init__ loads sys_dictionary and sys_db_object
# ---------------------------------------------------------------------------
class TestDisplayValueInit:

    def test_reads_sys_dictionary_table(self):
        mock_spark = MagicMock()
        from src.display import DisplayValue
        dv = DisplayValue(mock_spark, "cat", "sch", ["name"])

        # spark.read.table should have been called with the sys_dictionary path
        calls = [str(c) for c in mock_spark.read.table.call_args_list]
        assert any("sys_dictionary" in c for c in calls)

    def test_reads_sys_db_object_table(self):
        mock_spark = MagicMock()
        from src.display import DisplayValue
        dv = DisplayValue(mock_spark, "cat", "sch", ["name"])

        calls = [str(c) for c in mock_spark.read.table.call_args_list]
        assert any("sys_db_object" in c for c in calls)

    def test_stores_config_on_self(self):
        mock_spark = MagicMock()
        from src.display import DisplayValue
        dv = DisplayValue(mock_spark, "cat", "sch", ["name", "number"])

        assert dv.catalog == "cat"
        assert dv.schema == "sch"
        assert dv.sys_col_names == ["name", "number"]