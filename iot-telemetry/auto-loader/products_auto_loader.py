# Databricks notebook source
checkpointLocation ="/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/product/checkpoint/"
schemaLocation = "/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/product/schema/"
filePath = "/Volumes/iot-telemetry/raw/iot-telemetry-files/product/"

# COMMAND ----------

# MAGIC %run "./common_methods"

# COMMAND ----------

table_name="`iot-telemetry`.raw.products"

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