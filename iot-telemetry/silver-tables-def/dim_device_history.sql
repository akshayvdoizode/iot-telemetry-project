drop table if exists iot_telemetry.silver.dim_device_history ;
CREATE TABLE IF NOT EXISTS iot_telemetry.silver.dim_device_history (
    device_sk BIGINT GENERATED ALWAYS as IDENTITY,
    device_id STRING,
    serial_number STRING,
    product_number STRING,
    device_type STRING,
    asset_id STRING,

    installation_date DATE,
    commissioning_date DATE,
    location_zone STRING,
    operational_status STRING,
    maintenance_status STRING,
    last_maintenance_date DATE,
    warranty_expiry_date DATE,

    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN
)
USING DELTA;

