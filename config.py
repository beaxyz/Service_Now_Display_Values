# Catalog and schema of the destination of where to ingest the service now tables into. This should also be the same as the schema where sys_dict sits in.
catalog = "field_demos"
schema = "bliew_snow_test" # This is the schema where sys_dict sits in

# Schema of the source of where to ingest the service now tables from. Should be default
source_schema = "default"

# List of tables to ingest from Service Now. Only include the tables that are needed to be ingested. The reference tables will be added based on a lookup
table_list = [
    "asmt_metric_result",
    "asmt_assessment_instance",
    "change_request",
    "incident",
    "interaction",
    "problem",
    "sc_req_item",
    "sc_task",
    "task",
    "sys_user_group",
    "sysapproval_group",
    "sys_user",
    "task_sla",
]

# Display column names that are in service now tables
sys_col_names = ['name','u_name','number']

#Tables to be excluded from ingestion
tables_to_exclude = ['sys_package'] #Removing sys_package due to permissions insufficient issues

# Connection name of the service now connection to be used for ingestion
connection_name = "servicenow-latest"

# Pipeline name of the service now pipeline to be used for ingestion. If a lakeflow connect pipeline exists, then just replace this here and we will use this to update this existing pipeline
pipeline_name = "[dev beatrice_liew] servicenow_bliew_ingestion"