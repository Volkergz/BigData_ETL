#!/bin/bash
# ==============================================================================
# DEPLOYMENT SCRIPT: NYC TLC REAL-TIME EVENTS ENGINE (EVENT-DRIVEN ARCHITECTURE)
# ==============================================================================

set -eo pipefail

# 1. Detección Inteligente de Región (Org Policy Compliant)
echo "### [1/7] Autodetectando región bajo restricciones de Resource Location..."

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
echo "### [2/7] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export SA_NAME="sa-nyc-bq-loader"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SERVICE_NAME="gcs-to-bigquery-trigger"

# 3. Activación de APIs requeridas (Incluyendo Eventarc)
echo "### [3/7] Verificando y habilitando APIs de Google Cloud..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com

# 4. Creación de la Landing Zone en Storage
echo "### [4/7] Configurando almacenamiento en GCS (${REGION})..."
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" &>/dev/null; then
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --location="${REGION}" \
        --uniform-bucket-level-access
else
    echo " - El bucket gs://${BUCKET_NAME} ya existe."
fi

# 5. Seguridad de Identidad y Control de Accesos (IAM)
echo "### [5/7] Configurando Identidades y Permisos IAM (Bypass Mode)..."

if ! gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    gcloud iam service-accounts create "${SA_NAME}" --display-name="NYC BQ Loader SA"
fi

echo "Asignando permisos BigQuery Admin y Storage Admin a la Service Account..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.admin" > /dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin" > /dev/null

# 6. Despliegue como HTTP Cloud Function (Evita Eventarc y cuentas de sistema)
echo "### [6/7] Desplegando Servicio Cloud Run Gen 2 vía HTTP..."

gcloud functions deploy "${SERVICE_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=./data-raw/ \
    --entry-point=gcs_to_bigquery_trigger \
    --trigger-http \
    --timeout=600 \
    --memory=1Gi \
    --max-instances=10 \
    --service-account="${SA_EMAIL}" \
    --set-env-vars DATASET_ID=nyc_tlc_yellow,DATASET_LOCATION=US \
    --allow-unauthenticated # Requerido en laboratorios para recibir webhooks sin fricción

# 7. Vinculación Directa de Almacenamiento (Pub/Sub Notification Native Engine)
echo "### [7/7] Orquestando Webhook directo desde Cloud Storage..."

# 7.1 Obtener la URL de la función desplegada
FUNCTION_URL=$(gcloud functions describe "${SERVICE_NAME}" --gen2 --region="${REGION}" --format='value(serviceConfig.uri)')

# 7.2 Crear un tópico de Pub/Sub intermedio
TOPIC_NAME="nyc-gcs-events"
if ! gcloud pubsub topics describe "${TOPIC_NAME}" &>/dev/null; then
    gcloud pubsub topics create "${TOPIC_NAME}"
fi

# 7.3 Crear la suscripción tipo PUSH directo hacia la Cloud Function
SUB_NAME="nyc-gcs-push-sub"
if ! gcloud pubsub subscriptions describe "${SUB_NAME}" &>/dev/null; then
    gcloud pubsub subscriptions create "${SUB_NAME}" \
        --topic="${TOPIC_NAME}" \
        --push-endpoint="${FUNCTION_URL}"
fi

# 7.4 Vincular el Bucket con el Tópico de Pub/Sub de forma directa
# Nota: Esto no requiere la Service Account corrupta del sistema
if ! gcloud storage buckets notifications list "gs://${BUCKET_NAME}" | grep -q "${TOPIC_NAME}"; then
    gcloud storage buckets notifications create "gs://${BUCKET_NAME}" \
        --topic="${TOPIC_NAME}" \
        --event-types="OBJECT_FINALIZE"
fi

echo "### DESPLIEGUE FINALIZADO EXITOSAMENTE MEDIANTE BYPASS DE PROCESO ###"