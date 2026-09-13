# Databricks notebook source
checkpointLocation ="/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/device/checkpoint/"
schemaLocation = "/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/device/schema/"
filePath = "/Volumes/iot-telemetry/raw/iot-telemetry-files/device/"

# COMMAND ----------

# MAGIC %run "./common_methods"

# COMMAND ----------

table_name ="`iot-telemetry`.raw.devices"

# COMMAND ----------

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schemaLocation)
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .schema(device_schema)
        .load(filePath)
)

df = df.withColumn("created_at", current_timestamp())
query = (
    df.writeStream
        .option("checkpointLocation", checkpointLocation)
        .trigger(availableNow=True)
        .toTable(table_name)
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
#         .toTable("`iot-telemetry`.raw.device")
# )