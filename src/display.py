from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from config import *
from src.ingestion import IngestionManager
####LAKEFLOW DECLARATIVE PIPELINES######
# To build up a view of the display columns for each table: 

class DisplayValue:
  def __init__(self, spark, catalog, schema, sys_col_names, ingestion_manager = None):
    self.spark = spark
    self.catalog = catalog
    self.schema = schema
    self.sys_col_names = sys_col_names
    self.ingestion_manager = ingestion_manager


    self.df_sys_dict = self.spark.read.table(f"{self.catalog}.{self.schema}.sys_dictionary")
    self.df_sys_db_object = self.spark.read.table(f"{self.catalog}.{self.schema}.sys_db_object")

  #Check if the table has a display value = True flag in the sys_dictionary table
  def check_display_true(
    self,
    table_list 
  ):
    rows = (
        self.df_sys_dict.filter((F.col("display") == True) & (F.col("name").isin(table_list)))
        .select("name", "element", "display")
        .collect()
    )
    return rows

  # Get a list of elements of the table and cross check against the sys_col_names reference and return if it matches
  def get_elements(
    self, 
    table_list
    ) -> str:

    elements = (
        self.df_sys_dict.filter((F.col("name").isin(table_list)))
        .select("element")
        .distinct()
        .collect()
    )

    element_set = {row[0] for row in elements if row[0] and str(row[0]).strip()}
    display_col = next((col for col in self.sys_col_names if col in element_set), None)

    element_row = (self.df_sys_dict.filter(
        (F.col("name").isin(table_list)) & (F.col("element") == display_col)
    )
    .select("name")
    .first())
    
    if element_row is not None:
       element_table = element_row[0]

    else:
      element_table = None
    
    return element_table, display_col


  # Returns true if the display value is a reference field (ie: contains link)
  def check_if_display_is_reference(
    self,
    table,
    display_field  
  ) -> bool:
    table_full_name = f"{self.catalog}.{self.schema}.{table}"
    info_schema = (self.spark.read.table(f"{self.catalog}.information_schema.tables")
                  .filter(F.col("table_name")==table)
    )

    if info_schema.count()<1:
      print("Table does not exist")
      return None
    
    df_table = self.spark.read.table(table_full_name)
    if display_field in df_table.columns:
      display_col = df_table.select(F.col(display_field))
      
      if isinstance(display_col.schema[display_field].dataType, T.StructType):
        return True
      
      elif isinstance(display_col.schema[display_field].dataType, T.StringType):
        return False
    
    elif display_field not in df_table.columns:
      print("Col does not exist in table")
      return None


  # If the display field col is a reference field, then look up the reference table for the actual display col
  def return_display_value_if_reference(
    self,
    table,
    display_field,
    ingestion_manager = None
    ):

      if self.check_if_display_is_reference(table, display_field ):
          df_table = self.spark.read.table(f"{self.catalog}.{self.schema}.{table}")
          first_row = df_table.select(F.col(display_field)).filter(F.col(display_field).isNotNull()).first()
          if first_row is None or first_row[0] is None:
              print(f"Table '{table}' is empty or '{display_field}' is null in first row")
              return None
          display_col = first_row[0]

          link = (
              display_col.get("link")
              if isinstance(display_col, dict)
              else getattr(display_col, "link", None)
          )

          if link and "/table/" in link:
              reference_table = link.split("/table/")[-1].split("/")[0]
              a = df_table.alias("a")
              b = self.spark.read.table(f"{self.catalog}.{self.schema}.{reference_table}").alias("b")

              display_value_list = self.get_display_value(
                [reference_table],
                ingestion_manager
              )

              display_value = display_value_list[0]["element"]
              return {"reference_table": reference_table, "display_value": display_value}

      else:
          print("Display is not a reference field")
          
      
  # Get display value
  def get_display_value(
    self,
    table_list,
    ingestion_manager=None
    ) -> list(dict()):

      display_list = []
      im = ingestion_manager
      for table in table_list:

        # 1) Checks if the table in sys_dict has a display value = True flag. If so return the field. This includes the parents of the table
          table_with_parents = im.get_table_with_ancestors([table])
          table_dict = next(a for a in table_with_parents if a["table"] == table)
          # Limit to one level: table + direct parent only (avoids grandparents not in catalog, e.g. sttrm_model)
          table_with_parents_list = [table_dict["table"]]
          if "parent_lv_0" in table_dict:
            table_with_parents_list.append(table_dict["parent_lv_0"])
          # If parent equals table (self-reference in hierarchy, e.g. cmdb_ci_service), dedupe so we only query once and get one row
          print(table_with_parents_list)
          table_with_parents_list = list(dict.fromkeys(table_with_parents_list))

          rows = self.check_display_true(table_with_parents_list)
          if len(rows) <=1:
            for row in rows:
              # 1 a) Checks if the display field is a reference field. If yes, then look up the reference table for the display column
              is_display_reference_field = self.check_if_display_is_reference(table, row['element'])

              if is_display_reference_field:
                  reference_fields = self.return_display_value_if_reference(
                      table, row['element'], im
                  )
                  if reference_fields:  # <-- this is the new guard to handle None reference fields
                      display_value = reference_fields['display_value']
                      reference_table_for_display_value = reference_fields['reference_table']
                  else:
                      display_value = row['element']
                      reference_table_for_display_value = None
              else:
                  reference_table_for_display_value = None
                  display_value = row['element']


              display_list.append(
                  {
                      "table": table,
                      "element": row["element"],
                      "display_flag": row["display"],
                      "is_display_from_parent": True if table_with_parents_list[0] != row["name"] else False,
                      "parent_table": row["name"] if table_with_parents_list[0] != row["name"] else None,
                      "is_display_reference_field": is_display_reference_field,
                      "reference_table_for_display_value": reference_table_for_display_value,
                      "display_value":  display_value
                      }
              )

        # 2) If there is no display = True value, check if it has a name, u_name or number column. Assumption is that these fields are not reference fields
          if not rows:
            
            element_table, display_col = self.get_elements(table_with_parents_list)

            if display_col is not None:
              display_list.append(
                  {
                      "table": table,
                      "element": display_col,
                      "display_flag": False,
                      "is_display_from_parent": True if element_table != table else False,
                      "parent_table": element_table if element_table != table else None,
                      "is_display_reference_field": False,
                      "reference_table_for_display_value": None,
                      "display_value": display_col
                      }
              )

              #3) Else no display values, no parent & no element match
            else:
              display_list.append(
                {
                  "table": table,
                  "element": None,
                  "display_flag": False,
                  "is_display_from_parent": False,
                  "parent_table": None,
                  "is_display_reference_field": False,
                  "reference_table_for_display_value": None,
                  "display_value": None
                }
              )
                  
      return display_list
  
##### LAKEFLOW DECLARATIVE PIPELINE #####
# Function to join base tables to reference table based on a column being a reference column, taking the value from ref column and joining to sys_id in the ref table. If there is a reference field, then it does another join to extract the display column.
  def join_to_ref(
    self,
    base_df,
    row
  ):
    col_type = base_df.schema[row['sys_name']].dataType
    
    if row['element'] is not None and isinstance(col_type, T.StructType):
      ref_df = (self.spark.read.table(f"{self.catalog}.{self.schema}.{row['reference_table']}")
                .select(
                  F.col("sys_id").alias("ref_sys_id"), 
                  F.col(row['element']).alias(f"{row['sys_name']}_display")
                  )
                )
      
      table_ref_joined_df = (base_df
                            .join(
                              ref_df,
                              F.col(row['sys_name']).getField("value") == ref_df.ref_sys_id,
                                      'left')
                              .select(*base_df.columns, f"{row['sys_name']}_display"))
    

      if row['reference_table_for_display_value'] is not None:
        reference_table_for_display_value_df = (
          self.spark.read.table(f"{self.catalog}.{self.schema}.{row['reference_table_for_display_value']}")
                .select(
                  F.col("sys_id").alias("ref_sys_id"), 
                  F.col(row['display_value']).alias(f"{row['sys_name']}_ref_display")
                  )
                )
        
        table_ref_joined_df = (table_ref_joined_df.join(
          reference_table_for_display_value_df,
          F.col(f"{row['sys_name']}_display").getField("value") == reference_table_for_display_value_df.ref_sys_id,
          'left'
        )
        .drop(f"{row['sys_name']}_display", row['sys_name'], "ref_sys_id")
        .withColumnRenamed(f"{row['sys_name']}_ref_display", row['sys_name'])
        )
        
        return table_ref_joined_df
    
      else:
        table_ref_joined_df = (table_ref_joined_df
          .drop(row['sys_name'])
          .withColumnRenamed(f"{row['sys_name']}_display", row['sys_name'])
        )
        return table_ref_joined_df
      
    else: 
      return base_df