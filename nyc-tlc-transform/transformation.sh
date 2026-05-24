#!/usr/bin/env bash

# Salir inmediatamente si un comando falla
set -e

echo "========================================================================"
echo "Iniciando despliegue de ETL Serverless con Dataproc"
echo "========================================================================"

# 1. Region
echo "=== [1/4] Autodetectando región aprovisionada..."

# 1. Extraer la política de localización del proyecto actual
RAW_LOCATION=$(gcloud beta resource-manager org-policies describe gcp.resourceLocations \
    --project=$(gcloud config get-value project))

# 2. Extraer la región usando Regex:
DETECTED_REGION=$(echo "$RAW_LOCATION" | grep -oP '(?<=in:)[a-z0-9]+-[a-z0-9]+(?=-locations)' | head -n 1)

# 3. Fallback y exportación
if [[ -z "$DETECTED_REGION" ]]; then
    # Si no encuentra el formato 'in:XXX-locations', busca regiones simples (ej. us-central1)
    export REGION=$(echo "$RAW_LOCATION" | grep -oP '[a-z]+-[a-z0-9]+' | head -n 1)
else
    export REGION=$DETECTED_REGION
fi

# 4. Sincronización del entorno
gcloud config set compute/region $REGION
echo " - Región identificada y configurada: $REGION"

# 2. Configuración de Variables
echo "=== [2/4] Configurando variables de entorno..."

export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export DATASET_ID="nyc_analytics"
export PYSPARK_FILE="nyc-tlc-transform/taxi_transform.py"
export JOB_NAME="taxi-etl-job-$(date +%s)"

gcloud compute networks subnets update default \
    --region=$REGION \
    --enable-private-ip-google-access

# 3. Preparación del Codigo
echo "=== [3/4] Subiendo script PySpark a Cloud Storage... "
gsutil cp "$PYSPARK_FILE" "gs://$BUCKET_NAME/scripts/$PYSPARK_FILE"

# 4. Ejecución del Archivo en Dataproc
echo "=== [4/4] Ejecutando servicio Serverless en Dataproc..."

# El flag --jars inyecta el conector oficial de Google para BigQuery
gcloud dataproc batches submit pyspark "gs://$BUCKET_NAME/scripts/$PYSPARK_FILE" \
    --batch="$JOB_NAME" \
    --region="$REGION" \
    --deps-bucket="gs://$BUCKET_NAME" \
    --jars="spark-3.5-bigquery-0.44.1.jar" \
    --subnet="default" \
    -- \
    --project_id="$PROJECT_ID" \
    --dataset_id="$DATASET_ID" \
    --bucket_name="$BUCKET_NAME"

echo "===================================================================================="
echo "¡Proceso inicializado!= Los datos están siendo procesados e insertados en BigQuery."
echo "===================================================================================="