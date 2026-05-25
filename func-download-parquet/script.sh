#!/bin/bash
# ==============================================================================
# DEPLOYMENT SCRIPT: NYC TLC SYNC ENGINE (EXTERNAL CODE ARCHITECTURE)
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

# Fallback si no hay org policy restrictiva explícita
if [[ -z "$REGION" ]]; then
    export REGION=$(gcloud config get-value compute/region)
    [[ -z "$REGION" ]] && export REGION="us-central1"
fi

gcloud config set compute/region "$REGION"
echo " - Región configurada: $REGION"

# 2. Configuración de Variables Globales
echo "### [2/7] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export SA_NAME="sa-nyc-ingestor"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SERVICE_NAME="nyc-sync-engine"

# 3. Activación de APIs requeridas
echo "### [3/7] Verificando y habilitando APIs de Google Cloud..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com

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
echo "### [5/7] Configurando Identidades y Permisos IAM..."
if ! gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    gcloud iam service-accounts create "${SA_NAME}" --display-name="NYC Ingestor SA"
fi

# Vinculación del rol Storage Object User de manera idempotente
echo "Asignando permisos sobre el Bucket a la Service Account..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectUser" \
    --condition=None > /dev/null

# 6. Despliegue de la Cloud Function Gen 2 utilizando Código Externo
echo "### [6/7] Desplegando Servicio Cloud Run Gen 2 desde origen externo..."

# Nota: Se especifica '--source=.' apuntando al directorio raíz que contiene requirements.txt y la carpeta src/
gcloud functions deploy "${SERVICE_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=./func-download-parquet \
    --entry-point=sync_nyc_data \
    --trigger-http \
    --timeout=3600 \
    --memory=1Gi \
    --service-account="${SA_EMAIL}" \
    --set-env-vars LANDING_BUCKET="${BUCKET_NAME}" \
    --no-allow-unauthenticated \

# Otorgar permisos de invocación internos
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker" \
    --region="${REGION}" \
    --platform=managed > /dev/null

# 7. Orquestación con Cloud Scheduler
echo "### [7/7] Configurando trigger cronometrado en Cloud Scheduler..."
export FUNCTION_URL=$(gcloud functions describe "${SERVICE_NAME}" --gen2 --region="${REGION}" --format='value(serviceConfig.uri)')

# Re-creación limpia del Job del Scheduler
gcloud scheduler jobs delete nyc-tlc-monthly-sync --location="${REGION}" --quiet 2>/dev/null || true

gcloud scheduler jobs create http nyc-tlc-monthly-sync \
    --location="${REGION}" \
    --schedule="0 10 5 * *" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --message-body='{"type": "yellow"}' \
    --oidc-service-account-email="${SA_EMAIL}" \
    --time-zone="UTC"

echo "### DESPLIEGUE FINALIZADO EXITOSAMENTE EN LA REGIÓN: ${REGION} ###"