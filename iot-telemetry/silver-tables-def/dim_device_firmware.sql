drop table if exists iot_telemetry.silver.dim_device_firmware ;
CREATE TABLE IF NOT EXISTS iot_telemetry.silver.dim_device_firmware (
device_firmware_sk BIGINT GENERATED ALWAYS as IDENTITY,
device_id STRING,
firmware_version STRING,
effective_from TIMESTAMP,
effective_to TIMESTAMP,
is_current BOOLEAN
)
USING DELTA;

