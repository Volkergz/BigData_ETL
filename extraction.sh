#!/bin/bash
# ==============================================================================
# DEPLOYMENT SCRIPT: NYC TLC SYNC ENGINE (RE-ARCHITECTED FOR RESTRICTED POLICIES)
# ==============================================================================

set -e

# 1. Region
echo "### [1/8] Autodetectando región aprovisionada..."

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
echo "### [2/8] Configurando variables de entorno..."

export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME="ingesta-nyc-tlc-${PROJECT_ID}"
export SA_NAME="sa-nyc-ingestor"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SERVICE_NAME="nyc-sync-engine"

# 3. APIs
echo "### [3/8] Habilitando APIs..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com

# 4. Workspace
echo "### [4/8] Creando Workspace"
mkdir -p ~/nyc-ingestion-service && cd ~/nyc-ingestion-service

# [Bloques requirements.txt y main.py se mantienen igual que en la versión anterior]
cat <<EOF > requirements.txt
functions-framework==3.*
requests==2.31.0
google-cloud-storage==2.14.0
backoff==2.2.1
python-dateutil==2.8.2
EOF

cat <<EOF > main.py
import os
import requests
import backoff
import functions_framework
from google.cloud import storage
from datetime import datetime
from dateutil.relativedelta import relativedelta

BUCKET_NAME = os.environ.get("LANDING_BUCKET")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
START_DATE = datetime(2023, 1, 1)

def get_existing_blobs(prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
    return {os.path.basename(blob.name) for blob in blobs}

def generate_date_range():
    current = START_DATE
    end = datetime.now()
    while current <= end:
        yield current.year, f"{current.month:02d}"
        current += relativedelta(months=1)

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def download_file(url, blob_name):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    with requests.get(url, stream=True, timeout=60) as r:
        if r.status_code == 404: return False
        r.raise_for_status()
        blob.upload_from_file(r.raw, content_type='application/octet-stream')
    return True

@functions_framework.http
def sync_nyc_data(request):
    data = request.get_json(silent=True) or {}
    v_type = data.get('type', 'yellow')
    prefix = f"raw/nyc_tlc/{v_type}/"
    existing_files = get_existing_blobs(prefix)
    downloaded_count = 0
    for year, month in generate_date_range():
        file_name = f"{v_type}_tripdata_{year}-{month}.parquet"
        if file_name in existing_files: continue
        if download_file(f"{BASE_URL}/{file_name}", f"{prefix}{file_name}"):
            downloaded_count += 1
    return {"status": "completed", "new_files": downloaded_count}, 200
EOF

# 5. Infraestructura de Storage con Manejo de Errores Regionales
echo "### [5/8] Configurando Infraestructura en ${REGION}..."
# Crear bucket en la región asignada
gcloud storage buckets create gs://${BUCKET_NAME} \
    --location=${REGION} \
    --uniform-bucket-level-access || echo "Bucket ya existe o error de región"

# 6. Seguridad e Identidad
echo "### [6/8] Configurando IAM..."

# Asegurar que la Service Account existe antes de proseguir
if ! gcloud iam service-accounts describe ${SA_EMAIL} &>/dev/null; then
    gcloud iam service-accounts create ${SA_NAME} --display-name="NYC Ingestor SA"
fi

# Permiso para escribir en GCS Landing Zone
for i in {1..6}; do
    if gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/storage.objectUser" \
        --condition=None &>/dev/null; then
        echo "IAM vinculado con éxito."
        break
    else
        echo "Esperando propagación (intento $i/6)..."
        sleep 10
    fi
done

# 7. Despliegue de la Función
echo "### [7/8] Desplegando en Cloud Run (Gen 2)..."
gcloud functions deploy ${SERVICE_NAME} \
    --gen2 \
    --runtime=python311 \
    --region=${REGION} \
    --source=. \
    --entry-point=sync_nyc_data \
    --trigger-http \
    --timeout=3600 \
    --memory=1Gi \
    --service-account=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
    --set-env-vars LANDING_BUCKET=${BUCKET_NAME} \
    --no-allow-unauthenticated
    
# Asignación de Permisos de la función
echo "Otorgando permisos de invocador sobre el servicio de Cloud Run..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker" \
    --region=${REGION} \
    --platform=managed

# 8. Cloud Scheduler
echo "### [8/8] Configurando Scheduler..."
export FUNCTION_URL=$(gcloud functions describe ${SERVICE_NAME} --gen2 --region=${REGION} --format='value(serviceConfig.uri)')

# Re-creación limpia del job
gcloud scheduler jobs delete nyc-tlc-monthly-sync --location=${REGION} --quiet || true

gcloud scheduler jobs create http nyc-tlc-monthly-sync \
    --location=${REGION} \
    --schedule="0 10 5 * *" \
    --uri=${FUNCTION_URL} \
    --http-method=POST \
    --message-body='{"type": "yellow"}' \
    --oidc-service-account-email="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --time-zone="UTC"

echo "### DESPLIEGUE FINALIZADO EN ${REGION} ###"
