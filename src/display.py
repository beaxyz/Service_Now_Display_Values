from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from config import *

####LAKEFLOW DECLARATIVE PIPELINES######
# To build up a view of the display columns for each table: 

class DisplayValue:
  def __init__(self, spark, catalog, schema, sys_col_names):
    self.spark = spark
    self.catalog = catalog
    self.schema = schema
    self.sys_col_names = sys_col_names

    self.df_sys_dict = self.spark.read.table(f"{self.catalog}.{self.schema}.sys_dictionary")
    self.df_sys_db_object = self.spark.read.table(f"{self.catalog}.{self.schema}.sys_db_object")

  #Check if the table has a display value = True flag in the sys_dictionary table
  def check_display_true(
    self,
    table
  ):
    rows = (
        self.df_sys_dict.filter((F.col("display") == True) & (F.col("name") == table))
        .select("name", "element", "display")
        .collect()
    )
    return rows

  # Get a list of elements of the table and cross check against the sys_col_names reference and return if it matches
  def get_elements(
    self, 
    table
    ) -> str:
      elements = (
          self.df_sys_dict.filter((F.col("name") == table))
          .select("element")
          .distinct()
          .collect()
      )

      element_set = {row[0] for row in elements if row[0] and str(row[0]).strip()}
      display_col = next((col for col in self.sys_col_names if col in element_set), None)

      return display_col

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
    display_field
    ):

      if self.check_if_display_is_reference(table, display_field ):
          df_table = self.spark.read.table(f"{self.catalog}.{self.schema}.{table}")
          display_col = df_table.select(F.col(display_field)).first()[0]

          link = (
              display_col.get("link")
              if isinstance(display_col, dict)
              else getattr(display_col, "link", None)
          )

          if "/table/" in link:
              reference_table = link.split("/table/")[-1].split("/")[0]
              a = df_table.alias("a")
              b = self.spark.read.table(f"{self.catalog}.{self.schema}.{reference_table}").alias("b")

              display_value_list = self.get_display_value(
                [reference_table]
              )

              display_value = display_value_list[0]["element"]
              return {"reference_table": reference_table, "display_value": display_value}

      else:
          print("Display is not a reference field")
          
      
  # Get display value
  def get_display_value(
    self,
    table_list
    ) -> list(dict()):

      display_list = []
      for table in table_list:
        # 1) Checks if the table in sys_dict has a display value = True flag. If so return the field
          rows = self.check_display_true(table)

          for row in rows:
            # 1 a) Checks if the display field is a reference field. If yes, then look up the reference table for the display column
            is_display_reference_field = self.check_if_display_is_reference(row['name'],row['element'])

            if is_display_reference_field:
              reference_fields = self.return_display_value_if_reference(
                row['name'],
                row['element']
              )
              display_value = reference_fields['display_value']
              reference_table_for_display_value = reference_fields['reference_table']

            else:
              reference_table_for_display_value = None
              display_value = row['element']

            display_list.append(
                {
                    "table": row["name"],
                    "element": row["element"],
                    "display_flag": row["display"],
                    "has_parent": None,
                    "parent_table": None,
                    "is_display_reference_field": is_display_reference_field,
                    "reference_table_for_display_value": reference_table_for_display_value,
                    "display_value":  display_value
                    }
            )

          # 2) If there is no display = True value, check if it has a name, u_name or number column. Assumption is that these fields are not reference fields
          if not rows:
            
            display_col = self.get_elements(table)
            if display_col is not None:
              display_list.append(
                  {
                      "table": table,
                      "element": display_col,
                      "display_flag": False,
                      "has_parent": None,
                      "parent_table": None,
                      "is_display_reference_field": False,
                      "reference_table_for_display_value":None,
                      "display_value":display_col 
                      }
              )
            # If it has no name/number col & no display col        
            elif display_col is None:
              # Check for parent table
              parent = self.df_sys_db_object.filter(F.col("name")== table).select("super_class").first()

              # 2 a) If it has a parent class, look up the parent's display flag first. If it exists, check if it's a ref field.
              if parent is not None and parent['super_class'] is not None: 

                a = (self.df_sys_db_object
                      .filter(F.col("name")== table)
                      .select("name","super_class")
                      ).alias("a")
                
                b = self.df_sys_db_object.alias("b")

                parent_class = (a.join(b, 
                                        F.col("a.super_class.value") == F.col("b.sys_id")
                                        )
                                .select(
                                  F.col("b.name").alias("parent_table")
                                )).collect()[0]['parent_table']
            
                rows = self.check_display_true(parent_class)

                # Checks if the parent display value from sys_dict is a reference field. If yes, look up the actual field
                if len(rows)>=1:
                  for row in rows:
                    is_display_reference_field = self.check_if_display_is_reference(row['name'],row['element'])
                    
                    if is_display_reference_field:
                      reference_fields = self.return_display_value_if_reference(
                        row['name'],
                        row['element']
                      )

                    else:
                      reference_table_for_display_value = None
                      display_value = row['element']

                    display_list.append(
                        {
                          "table": table,
                          "element": None,
                          "display_flag": False,
                          "has_parent": "Y",
                          "parent_table": row['name'],
                          "is_display_reference_field": is_display_reference_field,
                          "reference_table_for_display_value":reference_table_for_display_value,
                          "display_value":  display_value
                          }
                        )

                #2 b) If parent table doesn't have display flag true in sys_dictionary, search for display value using elements in sys_dictionary that matches name, u_name & number.
                elif not rows:
                  parent_display_col = self.get_elements(parent_class)

                  display_list.append(
                  {
                      "table": table,
                      "element": None,
                      "display_flag": False,
                      "has_parent": "Y",
                      "parent_table": parent_class,
                      "is_display_reference_field": False,
                      "reference_table_for_display_value": None,
                      "display_value":  parent_display_col
                      })
          
              #3) Else no display values, no parent & no element match
              else:
                display_list.append(
                  {
                    "table": table,
                    "element": None,
                    "display_flag": False,
                    "has_parent": "N",
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
        .drop(f"{row['sys_name']}_display")
        .select(
          *[c for c in table_ref_joined_df.columns if c != f"{row['sys_name']}_display"],
          F.col(f"{row['sys_name']}_ref_display").alias(f"{row['sys_name']}_display"))
        )
        
        return table_ref_joined_df
    
      else:
        return table_ref_joined_df
      
    else: 
      return base_df