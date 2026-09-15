# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import to_date, col, try_to_date

# COMMAND ----------

catalog = "iot_telemetry"
schema = "raw"
table = "devices"
table_name = f"{catalog}.{schema}.{table}"

# COMMAND ----------

devices_df=spark.sql(f"""select * from {table_name}""")