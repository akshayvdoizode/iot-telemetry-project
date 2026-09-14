# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../common_methods"

# COMMAND ----------

folders = get_folders(
    rootPath,
    EXCLUDED_SOURCES
)

process_folders(
    folders,
    rootPath
)

# COMMAND ----------

# df = (
#     spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("cloudFiles.schemaLocation", schemaLocation)
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .load(filePath)
# )

# query = (
#     df.writeStream
#         .option("checkpointLocation", checkpointLocation)
#         .trigger(availableNow=True)
#         .toTable("`iot_telemetry`.raw.device")
# )