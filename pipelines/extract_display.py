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
from pyspark.sql import types as T
from pyspark.sql import functions as F
from src.display import DisplayValue
from src.ingestion import IngestionManager
from src.session import get_spark_and_client

_, w = get_spark_and_client()

@dp.materialized_view()
def display_value_mapping_table():
  dv = DisplayValue(spark, config.catalog, config.schema, config.sys_col_names)
  ingestion_manager = IngestionManager(spark, w,config.catalog, config.schema, config.source_schema, config.pipeline_name, config.connection_name)
  all_tables_to_be_ingested = ingestion_manager.full_table_ingestion_list(
    config.table_list
  )
  
  tables_to_exclude = config.tables_to_exclude
  all_tables_to_be_ingested = [t for t in all_tables_to_be_ingested if t not in tables_to_exclude]

  table_schema = "table STRING, display_flag BOOLEAN, element STRING, has_parent STRING, parent_table STRING, is_display_reference_field BOOLEAN, reference_table_for_display_value STRING, display_value STRING"

  return spark.createDataFrame(
    dv.get_display_value(
      all_tables_to_be_ingested
    ), 
    schema = table_schema)

def create_dp_table(
  table,
  dv,
  ingestion_manager
):
  _table = table

  @dp.materialized_view(
    name =f"{table}_display",
    comment=f"Display values extracted for {table}"
  )

  def _view():
    # Get the table + all its ancestor tables (e.g. incident -> [incident, task])
    ancestor_tables = list(
      ingestion_manager.get_table_with_ancestors([_table])[0].values()
    )

    df_sys_name_map = (dv.df_sys_dict
                      .filter(F.col("name").isin(ancestor_tables) & F.col("reference").isNotNull())
                      .select(F.col("element").alias("sys_name"), F.col("reference.value").alias("reference_table"))
                      .distinct())
    
    display_value_mapping_df = spark.table(f"{config.catalog}.{config.schema}.display_value_mapping_table")
    df_sys_name_display = (df_sys_name_map
                       .join(display_value_mapping_df, 
                             df_sys_name_map.reference_table == display_value_mapping_df.table
                             )
                       .withColumn("element", 
                                   F.when(
                                    F.col("element").isNull()&F.col("display_value").isNotNull(),
                                    F.col("display_value"))
                                   .otherwise(F.col("element")))
                       .select("sys_name","reference_table","element","reference_table_for_display_value","display_value"))
    
    base_df = spark.read.table(f"{config.catalog}.{config.schema}.{_table}")
    
    for row in df_sys_name_display.collect():
      base_df = dv.join_to_ref(
        base_df,
        row
        )
    
    return base_df
  
  globals()[f"{_table}_display"] = _view


dv = DisplayValue(spark, config.catalog, config.schema, config.sys_col_names)
ingestion_mgr = IngestionManager(spark, w, config.catalog, config.schema, config.source_schema, config.pipeline_name, config.connection_name)
for table in config.table_list:
  create_dp_table(
    table, 
    dv,
    ingestion_mgr
    )
    