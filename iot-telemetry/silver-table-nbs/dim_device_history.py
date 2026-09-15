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
print(table_name)

# COMMAND ----------

# Bronze source
devices_df = spark.sql(f"""
    SELECT
        device_id,
        device_serial_number AS serial_number,
        product_id AS product_number,
        device_type,
        asset_id,
        installation_date,
        commissioning_date,
        location_zone,
        operational_status,
        maintenance_status,
        last_maintenance_date,
        warranty_expiry_date,
        updated_at
    FROM {table_name}
""")

# COMMAND ----------

# Clean / cast
devices_df = devices_df.withColumns({
    "installation_date": to_date(col("installation_date"), "yyyy-MM-dd"),
    "commissioning_date": to_date(col("commissioning_date"), "yyyy-MM-dd"),
    "last_maintenance_date": to_date(col("last_maintenance_date"), "yyyy-MM-dd"),
    "warranty_expiry_date": to_date(col("warranty_expiry_date"), "yyyy-MM-dd")
})

clean_devices_df = devices_df.filter(
    col("device_id").isNotNull()
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Deduplicate
window_spec = (
    Window
    .partitionBy("device_id")
    .orderBy(col("updated_at").desc())
)

deduplicated_devices_df = (
    clean_devices_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

silver_table = "iot_telemetry.silver.dim_device_history"

# COMMAND ----------

# Current Silver state
current_devices_df = spark.sql(f"""
    SELECT
        device_id,
        installation_date,
        commissioning_date,
        effective_from,
        effective_to
    FROM {silver_table}
    WHERE is_current = TRUE
""")

# COMMAND ----------

device_comparison_df = (
    deduplicated_devices_df.alias("new")
    .join(
        current_devices_df.alias("old"),
        on="device_id",
        how="left"
    )
)

# COMMAND ----------

changed_devices_df = device_comparison_df.filter(
    col("old.device_id").isNotNull() &
    (
        ~col("new.installation_date").eqNullSafe(col("old.installation_date")) |
        ~col("new.commissioning_date").eqNullSafe(col("old.commissioning_date"))
    )
)

# COMMAND ----------

new_devices_df = device_comparison_df.filter(
    col("old.device_id").isNull()
)

# COMMAND ----------

unchanged_devices_df = device_comparison_df.filter(
    col("old.device_id").isNotNull() &
    col("new.installation_date").eqNullSafe(col("old.installation_date")) &
    col("new.commissioning_date").eqNullSafe(col("old.commissioning_date"))
)

# COMMAND ----------

print("New:", new_devices_df.count())
print("Changed:", changed_devices_df.count())
print("Unchanged:", unchanged_devices_df.count())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

initial_devices_df = (
    new_devices_df
    .select(
        col("new.device_id").alias("device_id"),
        col("new.serial_number").alias("serial_number"),
        col("new.product_number").alias("product_number"),
        col("new.device_type").alias("device_type"),
        col("new.asset_id").alias("asset_id"),
        col("new.installation_date").alias("installation_date"),
        col("new.commissioning_date").alias("commissioning_date"),
        col("new.location_zone").alias("location_zone"),
        col("new.operational_status").alias("operational_status"),
        col("new.maintenance_status").alias("maintenance_status"),
        col("new.last_maintenance_date").alias("last_maintenance_date"),
        col("new.warranty_expiry_date").alias("warranty_expiry_date")
    )
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# COMMAND ----------

initial_devices_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT device_id) AS distinct_devices,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
    FROM {silver_table}
""").show()