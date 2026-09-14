# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
import os

# COMMAND ----------

device_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("device_name", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

# COMMAND ----------

rootPath = "/Volumes/iot_telemetry/raw/iot-data"

# COMMAND ----------

EXCLUDED_SOURCES = {"configs"}

# COMMAND ----------

def create_autoloader_stream(schemaLocation, filePath):
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schemaLocation)
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .schema(device_schema)
        .load(filePath)
    )
    return df

# COMMAND ----------

def get_folders(rootPath,EXCLUDED_SOURCES):
    folders = [
        item.name.rstrip("/")
        for item in dbutils.fs.ls(rootPath)
        if item.isDir() and item.name.rstrip("/") not in EXCLUDED_SOURCES
    ]
    return folders

# COMMAND ----------

def write_bronze_table(df, checkpointLocation, table_name):

    df = df.withColumn(
        "created_at",
        current_timestamp()
    )

    query = (
        df.writeStream
            .option(
                "checkpointLocation",
                checkpointLocation
            )
            .trigger(availableNow=True)
            .toTable(table_name)
    )

    return query

# COMMAND ----------

def process_folders(folders, rootPath):

    successful_folders = []
    failed_folders = []

    for folder in folders:

        print("=" * 60)
        print(f"Processing source: {folder}")

        try:

            checkpointLocation = os.path.join(
                rootPath,
                "configs",
                folder,
                "checkpoint"
            )

            schemaLocation = os.path.join(
                rootPath,
                "configs",
                folder,
                "schema"
            )

            filePath = os.path.join(
                rootPath,
                folder
            )
            target_name="sensor_history" if folder == "telemetry" else folder
            table_name = f"iot_telemetry.raw.{target_name}"

            print(f"Source path      : {filePath}")
            print(f"Schema location  : {schemaLocation}")
            print(f"Checkpoint       : {checkpointLocation}")
            print(f"Target table     : {table_name}")

            print("Creating Auto Loader stream...")

            df = create_autoloader_stream(
                schemaLocation,
                filePath
            )

            print("Starting Bronze ingestion...")

            query = write_bronze_table(
                df,
                checkpointLocation,
                table_name
            )

            successful_folders.append(folder)

            print(f"SUCCESS: {folder}")
            print(f"Query ID: {query.id}")

        except Exception as e:

            failed_folders.append({
                "folder": folder,
                "error": str(e)
            })

            print(f"FAILED: {folder}")
            print(f"Error: {str(e)}")

            # Continue processing remaining folders
            continue

    print("\n")
    print("=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)

    print(f"Successful sources : {len(successful_folders)}")
    print(f"Failed sources     : {len(failed_folders)}")

    if successful_folders:
        print("\nSuccessful:")
        for folder in successful_folders:
            print(f"  ✓ {folder}")

    if failed_folders:
        print("\nFailed:")
        for failure in failed_folders:
            print(f"  ✗ {failure['folder']}")
            print(f"    {failure['error']}")

        raise RuntimeError(
            f"Ingestion failed for "
            f"{len(failed_folders)} source(s)."
        )

    print("\nAll sources processed successfully.")