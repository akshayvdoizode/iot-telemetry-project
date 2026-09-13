# Databricks notebook source
checkpointLocation ="/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/telemetry/checkpoint/"
schemaLocation = "/Volumes/iot-telemetry/raw/iot-telemetry-files/configs/telemetry/schema/"
filePath = "/Volumes/iot-telemetry/raw/iot-telemetry-files/telemetry/"

# COMMAND ----------

# MAGIC %run "./common_methods"

# COMMAND ----------

table_name="`iot-telemetry`.raw.telemetry"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

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

df = df.withColumn(
    "created_at",
    current_timestamp()
)

try:
    query = (
        df.writeStream
            .option("checkpointLocation", checkpointLocation)
            .trigger(availableNow=True)
            .toTable(table_name)
    )

except Exception as e:
    print(e)

# COMMAND ----------

