import config
from pyspark import pipelines as dp
from pyspark.sql import types as T
from pyspark.sql import functions as F
from helpers import *
import pandas as pd

df_sys_dict = spark.read.table(f"{config.catalog}.{config.schema}.sys_dictionary")
df_sys_db_object = spark.read.table(f"{config.catalog}.{config.schema}.sys_db_object")

@dp.materialized_view()
def display_value_mapping_table():
  all_tables_to_be_ingested = full_table_ingestion_list(
    spark,
    df_sys_dict,
    df_sys_db_object,
    table_list)
  
  tables_to_exclude = ['sys_package']
  all_tables_to_be_ingested = [t for t in all_tables_to_be_ingested if t not in tables_to_exclude]

  schema = "table STRING, display_flag BOOLEAN, element STRING, has_parent STRING, parent_table STRING, is_display_reference_field BOOLEAN, reference_table_for_display_value STRING, display_value STRING"

  return spark.createDataFrame(
    get_display_value(
      df_sys_dict, 
      df_sys_db_object,
      all_tables_to_be_ingested, 
      config.sys_col_names,
      config.catalog,
      config.schema
    ), 
    schema = schema)

def create_dp_table(
  table,
  df_sys_dict
):
  _table = table

  @dp.materialized_view(
    name =f"{table}_display",
    comment=f"Display values extracted for {table}"
  )

  def _view():
    df_sys_name_map = (df_sys_dict
                      .filter((F.col("name") == _table) & F.col("reference").isNotNull())
                      .select(F.col("element").alias("sys_name"), F.col("reference.value").alias("reference_table"))
                      .distinct())
    
    display_value_mapping_df = spark.table(f"{catalog}.{schema}.display_value_mapping_table")
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
    
    base_df = spark.read.table(f"{catalog}.{schema}.{_table}")
    
    for row in df_sys_name_display.collect():
      base_df = join_to_ref(
        base_df,
        row
        )
    
    return base_df
  
  globals()[f"{_table}_display"] = _view

for table in table_list:
  create_dp_table(
    table, 
    df_sys_dict
    )
    