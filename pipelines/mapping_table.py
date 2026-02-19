import sys, os

def _find_project_root():
    """Resolve the project root across local, serverless job, and DLT environments."""
    candidates = []
    # 1. Try __file__ (works locally)
    try:
        candidates.append(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
    # 2. Try frame introspection (works on serverless jobs)
    try:
        frame_file = sys._getframe(0).f_code.co_filename
        if frame_file and "<" not in frame_file:
            candidates.append(os.path.dirname(os.path.abspath(frame_file)))
    except Exception:
        pass
    # 3. Try cwd (often set to project root in DLT)
    candidates.append(os.getcwd())

    for d in candidates:
        root = os.path.join(d, "..")
        if os.path.isfile(os.path.join(root, "config.py")):
            return os.path.abspath(root)
    # Last resort: cwd itself might be the project root
    if os.path.isfile(os.path.join(os.getcwd(), "config.py")):
        return os.getcwd()
    return os.path.abspath(candidates[0] if candidates else os.getcwd())

sys.path.insert(0, _find_project_root())

import config
from pyspark import pipelines as dp
from src.display import DisplayValue
from src.ingestion import IngestionManager
from src.session import get_spark_and_client

spark, w = get_spark_and_client()

@dp.materialized_view()
def display_value_mapping_table():
  dv = DisplayValue(spark, config.catalog, config.schema, config.sys_col_names)
  ingestion_manager = IngestionManager(spark, w,config.catalog, config.schema, config.source_schema, config.pipeline_name, config.connection_name)
  all_tables_to_be_ingested = ingestion_manager.full_table_ingestion_list(
    config.table_list
  )
  
  tables_to_exclude = config.tables_to_exclude
  all_tables_to_be_ingested = [t for t in all_tables_to_be_ingested if t not in tables_to_exclude]

  table_schema = "table STRING, display_flag BOOLEAN, element STRING, is_display_from_parent BOOLEAN, parent_table STRING, is_display_reference_field BOOLEAN, reference_table_for_display_value STRING, display_value STRING"

  return spark.createDataFrame(
    dv.get_display_value(
      all_tables_to_be_ingested,
      ingestion_manager
    ), 
    schema = table_schema)
