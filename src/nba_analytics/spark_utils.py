"""Spark session helper that works in a Databricks job and from a laptop."""
from __future__ import annotations

import os


def get_spark():
    """Return a SparkSession.

    Inside a Databricks job/notebook the runtime's session is used. On a laptop,
    Databricks Connect (serverless) is used.
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless(True).getOrCreate()
