#!/usr/bin/env bash

# Manejo estricto de errores (Fail fast)
set -euo pipefail
IFS=$'\n\t'

echo "========== [START] Inicializando Despliegue en GCP =========="

# 1. Detección Inteligente de Región (Org Policy Compliant)
echo "### [1/8] Autodetectando región bajo restricciones de Resource Location..."

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

# 2. Configuración de Variables de Entorno
echo "### [2/8] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
export TOPIC_NAME="registros_compras"
export SUBSCRIPTION_NAME="${TOPIC_NAME}-subscription"

export SA_NAME="webhook-registros-compras"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SERVICE_NAME="webhook-registros-compras"

# Información de contexto para el usuario
echo "Proyecto: ${PROJECT_ID}"
echo "Región:   ${REGION}"

# 3. Activación de APIs requeridas
echo "### [3/8] Verificando y habilitando APIs de Google Cloud..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    run.googleapis.com \
    iam.googleapis.com

# 4. CREACIÓN DEL TOPIC EN PUB/SUB
echo "### [4/8] Creando el Topic ${TOPIC_NAME} (Pub/Sub)..."

# Verificar si el tópico ya existe para evitar colisiones
if ! gcloud pubsub topics describe "${TOPIC_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Creando tópico Pub/Sub: ${TOPIC_NAME}..."
    gcloud pubsub topics create "${TOPIC_NAME}" \
        --project="${PROJECT_ID}"
    echo "¡Tópico creado con éxito!"
else
    echo "El tópico ${TOPIC_NAME} ya existe. Omitiendo creación."
fi

# 5. CREACIÓN DE LA SUSCRIPCIÓN EN PUB/SUB
echo "### [5/8] Creando la Suscripción ${SUBSCRIPTION_NAME} en el Topic ${TOPIC_NAME} (Pub/Sub)..."

# Crear la suscripción correspondiente (Pull por defecto, útil para el checkpointing de Dataflow)
if ! gcloud pubsub subscriptions describe "${SUBSCRIPTION_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Creando suscripción: ${SUBSCRIPTION_NAME} vinculada al tópico: ${TOPIC_NAME}..."
    gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
        --topic="${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --ack-deadline=60 \
        --message-retention-duration=604800 # 7 días de retención para resiliencia ante caídas del pipeline
    echo "¡Suscripción creada con éxito!"
else
    echo "La suscripción ${SUBSCRIPTION_NAME} ya existe. Omitiendo creación."
fi

# 6. CREACIÓN DE LA CUENTA DE SERVICIO PARA LA CLOUD FUNCTION Y ASIGNACIÓN DE PERMISOS
echo "### [6/8] Creando Service Account para la Cloud Function y asignando permisos necesarios..."

# Creación del Service Account para la Cloud Function (Principio de Menor Privilegio)
if ! gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Creando Service Account para la Cloud Function..."
    
    gcloud iam service-accounts create "${SA_NAME}" \
        --description="Service account para validar webhook y publicar en Pub/Sub" \
        --display-name="SA Webhook PubSub" \
        --project="${PROJECT_ID}"
    sleep 5 # Espera de cortesía para propagación de IAM

    # Otorgar permisos para publicar en el tópico específico
    echo "Asignando rol roles/pubsub.publisher sobre el tópico..."
    
    gcloud pubsub topics add-iam-policy-binding "${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/pubsub.publisher" >/dev/null
else
    echo "El Service Account ${SA_EMAIL} ya existe."
fi

# 7. CREACIÓN DE LA CLOUD FUNCTION
echo "### [7/8] Desplegando la Cloud Function ${SERVICE_NAME}..."

# Definir el directorio de origen del código de la función (donde se encuentra el Dockerfile)
export FUNCTION_SOURCE_DIR="./Streaming/func-create-webhook"

# 7.1 Validación de existencia de artefactos en el Repositorio
if [[ ! -f "${FUNCTION_SOURCE_DIR}/main.py" || ! -f "${FUNCTION_SOURCE_DIR}/requirements.txt" ]]; then
    echo "CRITICAL ERROR: No se encontraron los archivos main.py o requirements.txt en la ruta: ${FUNCTION_SOURCE_DIR}" >&2
    echo "Asegúrate de ejecutar este script desde la raíz del repositorio o ajustar FUNCTION_SOURCE_DIR." >&2
    exit 1
fi

# 7.2 Despliegue de la Cloud Function (Gen 2) apuntando al código del repositorio
echo "Desplegando Cloud Function ${SERVICE_NAME} desde origen: ${FUNCTION_SOURCE_DIR}..."
gcloud functions deploy "${SERVICE_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --trigger-http \
    --entry-point=main \
    --source="${FUNCTION_SOURCE_DIR}" \
    --service-account="${SA_EMAIL}" \
    --max-instances=10 \
    --no-allow-unauthenticated \
    --timeout=3600 \
    --memory=512Mi \
    --set-env-vars PROJECT_ID="${PROJECT_ID}",TOPIC_NAME="${TOPIC_NAME}" \
    --quiet

echo "¡Cloud Function ${SERVICE_NAME} desplegada con éxito!"

# 8. APERTURA DEL WEBHOOK PARA LLAMADAS EXTERNAS (INGRESS) ──────────────
echo " [8/8] Modificando políticas de IAM en Cloud Run para permitir tráfico público..."

# Si el servicio subyacente se llama exactamente igual que la variable ${SERVICE_NAME}
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --member="allUsers" \
    --role="roles/run.invoker" \
    --quiet

echo " [OK] Webhook configurado como Público Expuesto de forma segura."
echo "========== [DEPLOYMENT SUCCESSFUL] =========="