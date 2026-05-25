#!/bin/bash
# ==============================================================================
# DEPLOYMENT SCRIPT: NYC TLC REAL-TIME EVENTS ENGINE (EVENT-DRIVEN ARCHITECTURE)
# ==============================================================================

set -eo pipefail

# 1. Detección Inteligente de Región (Org Policy Compliant)
echo "### [1/6] Autodetectando región bajo restricciones de Resource Location..."

RAW_LOCATION=$(gcloud beta resource-manager org-policies describe gcp.resourceLocations \
    --project="$(gcloud config get-value project)" 2>/dev/null || echo "")

if [[ -n "$RAW_LOCATION" ]]; then
    DETECTED_REGION=$(echo "$RAW_LOCATION" | grep -oP '(?<=in:)[a-z0-9]+-[a-z0-9]+(?=-locations)' | head -n 1)
    if [[ -z "$DETECTED_REGION" ]]; then
        export REGION=$(echo "$RAW_LOCATION" | grep -oP '[a-z]+-[a-z0-9]+' | head -n 1)
    else
        export REGION=$DETECTED_REGION
    fi
fi

if [[ -z "$REGION" ]]; then
    export REGION=$(gcloud config get-value compute/region)
    [[ -z "$REGION" ]] && export REGION="us-central1"
fi

gcloud config set compute/region "$REGION"
echo " - Región configurada: $REGION"

# 2. Configuración de Variables Globales
echo "### [2/6] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export SA_NAME="sa-nyc-bq-loader"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SERVICE_NAME="gcs-to-bigquery-trigger"

# 3. Activación de APIs requeridas (Incluyendo Eventarc)
echo "### [3/6] Verificando y habilitando APIs de Google Cloud..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com

# 4. Creación de la Landing Zone en Storage
echo "### [4/6] Configurando almacenamiento en GCS (${REGION})..."
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" &>/dev/null; then
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --location="${REGION}" \
        --uniform-bucket-level-access
else
    echo " - El bucket gs://${BUCKET_NAME} ya existe."
fi

# 5. Seguridad de Identidad y Control de Accesos (IAM)
echo "### [5/6] Configurando Identidades y Permisos IAM..."

# 5.1 Cuenta de servicio de la Cloud Function
if ! gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    gcloud iam service-accounts create "${SA_NAME}" --display-name="NYC BQ Loader SA"
fi

echo "Asignando permisos BigQuery Admin y Storage Admin a la Service Account de la función..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.admin" \
    --condition=None > /dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin" \
    --condition=None > /dev/null

# 5.2 Autenticación del Agente Eventarc para evitar el error 403 Validation Failed
echo "Inyectando políticas IAM al Agente de Eventarc del sistema..."
export EVENTARC_SA="service-${PROJECT_NUMBER}@gcp-sa-eventarc.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/eventarc.serviceAgent" \
    --condition=None > /dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/pubsub.publisher" \
    --condition=None > /dev/null

# 5.3 Permiso para que la cuenta de almacenamiento envíe tokens a Pub/Sub
echo "Inicializando y autorizando la Service Account interna de Cloud Storage..."

# Forzar a GCP a crear/retornar la cuenta de servicio interna de GCS para este proyecto
export STORAGE_SA=$(gcloud storage service-agent --project="${PROJECT_ID}")

echo "Asignando rol Pub/Sub Publisher a la SA de Storage: ${STORAGE_SA}"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${STORAGE_SA}" \
    --role="roles/pubsub.publisher" \
    --condition=None > /dev/null

# 6. Despliegue de la Cloud Function Gen 2 Orientada a Eventos
echo "### [6/6] Desplegando Servicio Cloud Run Gen 2 con Trigger de Eventarc..."

gcloud functions deploy "${SERVICE_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=./nyc-tlc-extractor \
    --entry-point=gcs_to_bigquery_trigger \
    --trigger-event-resource="projects/_/buckets/${BUCKET_NAME}" \
    --trigger-event=google.cloud.storage.object.v1.finalized \
    --timeout=600 \
    --memory=1Gi \
    --max-instances=10 \
    --service-account="${SA_EMAIL}" \
    --set-env-vars DATASET_ID=nyc_tlc_yellow,DATASET_LOCATION=US

echo "### DESPLIEGUE EVENT-DRIVEN FINALIZADO EXITOSAMENTE EN LA REGIÓN: ${REGION} ###"
echo "Estructura lista. Cada archivo Parquet que caiga en gs://${BUCKET_NAME}/raw/nyc_tlc/yellow/ generará una tabla optimizada en BigQuery."