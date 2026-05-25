#!/usr/bin/env bash

# 1. Region
echo "=== [1/4] Autodetectando región aprovisionada..."

RAW_LOCATION=$(gcloud beta resource-manager org-policies describe gcp.resourceLocations \
    --project=$(gcloud config get-value project))

DETECTED_REGION=$(echo "$RAW_LOCATION" | grep -oP '(?<=in:)[a-z0-9]+-[a-z0-9]+(?=-locations)' | head -n 1)

if [[ -z "$DETECTED_REGION" ]]; then
    # Si no encuentra el formato 'in:XXX-locations', busca regiones simples (ej. us-central1)
    export REGION=$(echo "$RAW_LOCATION" | grep -oP '[a-z]+-[a-z0-9]+' | head -n 1)
else
    export REGION=$DETECTED_REGION
fi

gcloud config set compute/region $REGION
echo " - Región identificada y configurada: $REGION"

# 2. Configuración de Variables
echo "=== [2/4] Configurando variables de entorno..."

export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export DATASET_ID="nyc_raw"

# 3. Habilitar APIs
echo "=== [3/4] Habilitando APIs..."
# Habilitar Eventarc y dependencias de Cloud Functions v2
gcloud services enable \
    eventarc.googleapis.com \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com

# 4. Desplegar Cloud Function
echo "=== [4/4] Desplegando Cloud Function..."

gcloud functions deploy gcs-nyc-tlc-extractor \
    --gen2 \
    --source="./data-raw/" \
    --runtime=python311 \
    --region=$REGION \
    --entry-point=gcs_to_bigquery_trigger \
    --trigger-resource="projects/_/buckets/$BUCKET_NAME" \
    --trigger-event=google.cloud.storage.object.v1.finalized \
    --set-env-vars DATASET_ID="$DATASET_ID",DATASET_LOCATION=US \
    --max-instances=10 \
    --run-service-account="$(gcloud config get-value project)@appspot.gserviceaccount.com"