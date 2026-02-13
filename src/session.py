import os
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient


def get_spark_and_client(profile="dogfood"):
    """Return (spark, WorkspaceClient) configured for the current environment.

    On Databricks (DATABRICKS_RUNTIME_VERSION is set):
      - spark via SparkSession.builder.getOrCreate()
      - WorkspaceClient() with auto-auth

    Locally:
      - spark via DatabricksConnect with the given profile
      - WorkspaceClient(profile=profile)
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        spark = SparkSession.builder.getOrCreate()
        w = WorkspaceClient()
    else:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile(profile).serverless().getOrCreate()
        w = WorkspaceClient(profile=profile)
    return spark, w


      