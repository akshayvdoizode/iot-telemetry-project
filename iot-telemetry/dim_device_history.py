# Databricks notebook source
from pyspark.sql.functions import to_date, col, try_to_date

# COMMAND ----------

catalog = "iot_telemetry"
schema = "raw"
table = "devices"
table_name = f"{catalog}.{schema}.{table}"

# COMMAND ----------

devices_df = spark.sql(f"""
                     select device_id,
                            device_serial_number as serial_number,
                            product_id as product_number,
                            device_type,
                            asset_id,
                            installation_date,
                            commissioning_date,
                            location_zone,
                            operational_status
                            ,maintenance_status,
                            last_maintenance_date,
                            warranty_expiry_date,
                            updated_at
                     from {table_name}
                     """)

# COMMAND ----------

display(devices_df)

# COMMAND ----------

devices_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date

devices_df = devices_df.withColumns(
    {
        "installation_date": to_date(col("installation_date"), "yyyy-MM-dd"),
        "commissioning_date": to_date(col("commissioning_date"), "yyyy-MM-dd"),
        "last_maintenance_date": to_date(col("last_maintenance_date"), "yyyy-MM-dd"),
        "warranty_expiry_date": to_date(col("warranty_expiry_date"), "yyyy-MM-dd"),
    }
)

# COMMAND ----------

filterd_devices_df = devices_df.filter(col("device_id").isNotNull())

# COMMAND ----------

filterd_devices_df.printSchema()
filterd_devices_df.show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("device_id").orderBy(col("updated_at").desc())
filterd_devices_df = (
    filterd_devices_df.withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

silver_table = "iot_telemetry.silver.dim_device_history"

# COMMAND ----------

old_devices_df = spark.sql(f"""select device_id,installation_date,commissioning_date,effective_from,effective_to from {silver_table}
                         where is_current=TRUE""")

# COMMAND ----------

joined_df = filterd_devices_df.alias("new").join(
    old_devices_df.alias("old"), on="device_id", how="left"
)

# COMMAND ----------

changed_df = joined_df.filter(
    (~col("new.installation_date").eqNullSafe(col("old.installation_date"))) |
    (~col("new.commissioning_date").eqNullSafe(col("old.commissioning_date")))
)

# COMMAND ----------

display(changed_df)