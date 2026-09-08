from pyspark.sql import functions as F
from config import *

####LAKEFLOW CONNECT#####

class IngestionManager:
  def __init__(self, spark, w,catalog, schema, source_schema, pipeline_name, connection_name):
    self.spark = spark
    self.w = w
    self.catalog = catalog
    self.schema = schema
    self.source_schema = source_schema
    self.pipeline_name = pipeline_name
    self.connection_name = connection_name

    # Read in the sys_dictionary and sys_db_object tables once and reuse
    self.df_sys_dict = spark.read.table(f"{catalog}.{schema}.sys_dictionary")
    self.df_sys_db_object = spark.read.table(f"{catalog}.{schema}.sys_db_object")

  # Get the ancestors of each table (walks super_class recursively)
  def get_table_with_ancestors(self, table_list):
    """Return a list of dicts like [{"table": "incident", "parent_lv_0": "task", "parent_lv_1": "..."}, ...]
    Levels grow dynamically based on ancestry depth."""
    table_list_with_parents = []
    for table in table_list:
      ancestors = {"table": table}
      current = table
      
      for i in range(10):  # safety limit
        parent_row = (self.df_sys_db_object
            .filter(F.col("name") == current)
            .select("super_class")
            .first())
        if parent_row and parent_row["super_class"] is not None:
          parent = (self.df_sys_db_object
              .filter(F.col("sys_id") == parent_row["super_class"]["value"])
              .select("name")
              .first())
          if parent and parent["name"]:
            ancestors[f"parent_lv_{i}"] = parent["name"]
            current = parent["name"]
          else:
            break
        else:
          break
      
      table_list_with_parents.append(ancestors)
    
    return table_list_with_parents

    # Retrieve reference tables needed by core tables to be ingested from Service Now
  def get_reference_tables(
    self, 
    table_list
    )-> dict[str, list[str]]:

    parent_table_list = self.get_table_with_ancestors(table_list)
    
    ref_table = []
    for table in table_list:
      table_dict = next(a for a in parent_table_list if a["table"] == table)
      parent_tables = list(table_dict.values())
      
      rows = (self.df_sys_dict.filter(
                (F.col("internal_type.value") == "reference") & (F.col("name").isin(parent_tables))
            )
            .select("reference.value","element", "display")
            .collect())

      ref_list = [{"ref_table": row[0], "ref_field": row[1], "display": row[2]} for row in rows]

      ref_table.append({"table": table, "reference_values": ref_list})
    return ref_table



  # Retrieve the full list of tables to be ingested from Service Now
  def full_table_ingestion_list(
    self,
    table_list
  ):
    reference_table_list = [
      row.ref_table
      for row in (
          self.spark.createDataFrame(self.get_reference_tables(table_list))
          .withColumn("ref_values", F.explode(F.col("reference_values")))
          .withColumn("ref_table", F.col("ref_values.ref_table"))
          .drop("reference_values", "ref_values")
      ).collect()]
    
    table_and_ref_table = list(dict.fromkeys(table_list + reference_table_list))
    
    # Get ancestors for all tables, extract flat list of unique parent names
    ancestors_list = self.get_table_with_ancestors(table_and_ref_table)
    parent_table_list = list(dict.fromkeys(
      parent
      for ancestors in ancestors_list
      for key, parent in ancestors.items()
      if key != "table"  # skip the "table" key, keep only parent_lv_N values
    ))
    
    return list(dict.fromkeys(table_list + reference_table_list))


  def get_existing_pipeline_tables(self):
      """Return the set of tables currently in the pipeline."""
      for p in self.w.pipelines.list_pipelines():
          if p.name == self.pipeline_name:
              pipeline = self.w.pipelines.get(p.pipeline_id)
              return {
                  obj.table.source_table
                  for obj in pipeline.spec.ingestion_definition.objects
              }
      return set()

  # Create a Lakeflow Connect Ingestion Pipeline that ingests the list of tables & reference tables required
  def create_or_update_servicenow_pipeline(
  self,
  table_list
  ):
    """Create or Update an existing pipeline with the new list of tables."""
    from databricks.sdk.service.pipelines import (
    IngestionPipelineDefinition,
    IngestionConfig,
    TableSpec)
    
    objects = [
    IngestionConfig(
      table = TableSpec(
        source_table=table,
        source_schema=self.source_schema,
        destination_catalog=self.catalog,
        destination_schema=self.schema
        )
      )
    for table in table_list
    ]
    
    ingestion_pipeline_def = IngestionPipelineDefinition(
        connection_name=self.connection_name,
        objects=objects,
    )
    existing_pipeline = next(
        (p for p in self.w.pipelines.list_pipelines() if p.name == self.pipeline_name),
        None,
    )

    if existing_pipeline:
      self.w.pipelines.update(
              pipeline_id=existing_pipeline.pipeline_id,
              name=existing_pipeline.name,
              catalog=self.catalog,
              schema=self.schema,
              ingestion_definition=ingestion_pipeline_def,
              serverless=True,
            )
      print(f"Updated pipeline {existing_pipeline.pipeline_id} with {len(table_list)} tables")
      return existing_pipeline.pipeline_id

    else:
      created = self.w.pipelines.create(
        name=self.pipeline_name,
        catalog=self.catalog,
        schema=self.schema,
        ingestion_definition=ingestion_pipeline_def,
        serverless=True,
      )
      print(f"Created pipeline {self.pipeline_name} with {len(table_list)} tables")
      return created.pipeline_id


