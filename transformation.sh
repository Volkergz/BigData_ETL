#!/bin/bash

# Exit inmediatamente si un comando falla
set -e

echo "=== [1/4] CONFIGURANDO VARIABLES DE ENTORNO ==="
export PROJECT_ID=$(gcloud config get-value project)
export DATASET_NAME="nyc_analytics"
export TABLE_NAME="yellow_trips_materialized"

export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"

echo "Proyecto Actual: ${PROJECT_ID}"
echo "Bucket de Origen: gs://${BUCKET_NAME}"

echo ""
echo "=== [2/4] CREANDO EL DATASET EN LA UBICACIÓN 'US' ==="
bq --location=US mk --dataset "${PROJECT_ID}:${DATASET_NAME}"

echo "=== [3/4] CREANDO LA TABLA DE DESTINO OPTIMIZADA ==="
# Creamos la estructura física final limpia con tipos de datos estrictos
bq query --use_legacy_sql=false "
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET_NAME}.${TABLE_NAME}\`
(
  vendor_id STRING,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT64,
  trip_distance FLOAT64,
  rate_code_id INT64,
  rate_code_description STRING,
  payment_type INT64,
  fare_amount FLOAT64,
  tip_amount FLOAT64,
  total_amount FLOAT64,
  pulocation_id INT64,
  dolocation_id INT64
)
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY vendor_id, rate_code_id;
"

echo ""
echo "=== [4/4] CARGANDO E INYECTANDO DATOS DE LOS PARQUETS ==="
echo "1. Creando tabla externa temporal tolerante a fallos..."

# Separamos la creación de la tabla externa para asegurar que BigQuery aplique el esquema explícito
bq query --use_legacy_sql=false "
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.${DATASET_NAME}.tmp_parquet_source\` (
  VendorID INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  RatecodeID FLOAT64,
  payment_type FLOAT64,
  fare_amount FLOAT64,
  tip_amount FLOAT64,
  total_amount FLOAT64,
  PULocationID INT64,
  DOLocationID INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${BUCKET_NAME}/raw/nyc_tlc/yellow/*.parquet']
);"

echo "2. Inyectando y normalizando flujos de datos en la tabla productiva..."
# Ejecutamos la transferencia y el mapeo de categorías
bq query --use_legacy_sql=false "
INSERT INTO \`${PROJECT_ID}.${DATASET_NAME}.${TABLE_NAME}\` (
  vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  rate_code_id,
  rate_code_description,
  payment_type,
  fare_amount,
  tip_amount,
  total_amount,
  pulocation_id,
  dolocation_id
)
SELECT
  CAST(VendorID AS STRING) AS vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  SAFE_CAST(passenger_count AS INT64) AS passenger_count,
  CAST(trip_distance AS FLOAT64) AS trip_distance,
  SAFE_CAST(RatecodeID AS INT64) AS rate_code_id,
  
  CASE SAFE_CAST(RatecodeID AS INT64)
    WHEN 1 THEN 'Standard rate'
    WHEN 2 THEN 'JFK'
    WHEN 3 THEN 'Newark'
    WHEN 4 THEN 'Nassau or Westchester'
    WHEN 5 THEN 'Negotiated fare'
    WHEN 6 THEN 'Group ride'
    ELSE 'Unknown'
  END AS rate_code_description,
  
  SAFE_CAST(payment_type AS INT64) AS payment_type,
  CAST(fare_amount AS FLOAT64) AS fare_amount,
  CAST(tip_amount AS FLOAT64) AS tip_amount,
  CAST(total_amount AS FLOAT64) AS total_amount,
  SAFE_CAST(PULocationID AS INT64) AS pulocation_id,
  SAFE_CAST(DOLocationID AS INT64) AS dolocation_id
FROM
  \`${PROJECT_ID}.${DATASET_NAME}.tmp_parquet_source\`
WHERE
  tpep_pickup_datetime IS NOT NULL
  AND total_amount > 0;"

echo "3. Limpiando metadatos temporales..."
bq query --use_legacy_sql=false "DROP TABLE IF EXISTS \`${PROJECT_ID}.${DATASET_NAME}.tmp_parquet_source\`;"

echo ""
echo "=== ¡PIPELINE DE TRANSFORMACIÓN FINALIZADO CON ÉXITO! ==="