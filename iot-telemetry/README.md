# iot-telemetry

## Getting Started

To deploy and manage this bundle, follow these steps:

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Jobs & Pipelines

- To run a deployed job or pipeline, hover over the resource in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Add** dropdown to add resources to the bundle.
- Click **Schedule** on a notebook within the bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Declarative Automation Bundles in the workspace**, see: [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Declarative Automation Bundles format** used in this bundle, see: [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)

dim_device_history
────────────────────────────────────
device_sk
device_id
serial_number
product_number
device_type_key
model_sk
asset_key

installation_date_key
commissioning_date_key
location_zone
operational_status
maintenance_status
last_maintenance_date_key
warranty_expiry_date_key

effective_from
effective_to
is_current

dim_device_firmware
────────────────────────────────────
device_firmware_sk
device_id
firmware_version

effective_from
effective_to
is_current

dim_device_model
────────────────────────────────────
model_sk
product_number
model_number
manufacturer
device_type_key
hardware_revision
device_name

dim_device_type
────────────────────────────────────
device_type_key
device_type_code
device_type_name

1 | TEMP_HUM_PRESS | Temperature + Humidity + Pressure
2 | CURR_VOLT_TEMP | Current + Voltage + Temperature
3 | PRESS_TEMP     | Pressure + Temperature
4 | VIB_TEMP       | Vibration + Temperature
5 | TEMP_HUM       | Temperature + Humidity
6 | VIB_PRESS_TEMP | Vibration + Pressure + Temperature