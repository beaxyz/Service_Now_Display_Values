import sys, os
# Add project root to path so config and src are importable
# __file__ works locally; on Databricks serverless, fall back to co_filename from exec/compile
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys._getframe(0).f_code.co_filename))
sys.path.insert(0, os.path.join(_script_dir, ".."))

from config import *
from src.session import get_spark_and_client
from src.ingestion import IngestionManager

def main():

    spark, w = get_spark_and_client()
    mgr = IngestionManager(spark, w, catalog, schema, source_schema, pipeline_name, connection_name)

    all_tables_to_be_ingested = mgr.full_table_ingestion_list(
    table_list
    )

    all_tables_to_be_ingested = [t for t in all_tables_to_be_ingested if t not in tables_to_exclude]

    pipeline_id = mgr.create_or_update_servicenow_pipeline(
            table_list=all_tables_to_be_ingested
        )
    print(f"Pipeline created or updated with ID: {pipeline_id}")

if __name__ == "__main__":
    main()