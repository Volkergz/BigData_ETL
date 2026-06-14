#!/usr/bin/env bash

# Manejo estricto de errores (Fail fast)
set -euo pipefail
IFS=$'\n\t'

echo "========== [START] Inicializando Pipeline Analítico (Pub/Sub -> BigQuery) =========="

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

# 2. Configuración de Variables de Entorno
echo "### [2/7] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
# Origenes de datos
export TOPIC_NAME="registros_compras"
export SUBSCRIPTION_NAME="${TOPIC_NAME}-subscription"
# Destino Analítico en BigQuery
export BQ_DATASET="analytics_compras"
export BQ_TABLE="raw_registros_compras"

# Infraestructura Operativa de Dataflow
export GCS_BUCKET_NAME="${PROJECT_ID}-dataflow-staging"
export DATAFLOW_JOB_NAME="df-stream-pubsub-to-bq-prod"
export SA_DATAFLOW="sa-dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com"

echo " Proyecto:    ${PROJECT_ID}"
echo " Región:      ${REGION}"
echo " Suscripción: ${SUBSCRIPTION_NAME}"
echo " Destino BQ:  ${BQ_DATASET}.${BQ_TABLE}"

# 3. HABILITACIÓN DE APIS NECESARIAS PARA EL STACK ANALÍTICO
echo "### [3/7] Verificando y habilitando APIs de Google Cloud..."

gcloud services enable \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    compute.googleapis.com \
    pubsub.googleapis.com \
    iam.googleapis.com \
    --quiet

echo " [OK] APIs del stack analítico activadas correctamente."


# 3. CREACIÓN DEL DATASET Y TABLA OPTIMIZADA EN BIGQUERY
echo "### [4/7] Configurando almacenamiento optimizado en BigQuery..."

# 3.1 Crear Dataset si no existe
if ! bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1; then
    echo " Creando Dataset: ${BQ_DATASET}..."
    bq --location="${REGION}" mk --dataset \
        --description="Dataset para analítica avanzada consumida por Looker" \
        "${PROJECT_ID}:${BQ_DATASET}"
else
    echo " El Dataset ${BQ_DATASET} ya existe."
fi

# 3.2 Crear Tabla con Particionamiento y Clustering si no existe
if ! bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}.${BQ_TABLE}" >/dev/null 2>&1; then
    echo " Creando Tabla Optimizada: ${BQ_TABLE}..."
    
    # 1. Definición del Esquema basado en el Payload del Webhook
    SCHEMA_DEFINITION="id_cliente:STRING,\
        cliente:STRING,\
        genero:STRING,\
        id_producto:STRING,\
        producto:STRING,\
        precio:NUMERIC,\
        cantidad:INTEGER,\
        monto:NUMERIC,\
        forma_pago:STRING,\
        fecreg:TIMESTAMP"

    # 2. Creación de la tabla aplicando Ingeniería de Rendimiento para Looker
    bq mk --table \
        --project_id="${PROJECT_ID}" \
        --time_partitioning_type=DAY \
        --time_partitioning_field=fecreg \
        --clustering_fields="id_cliente,id_producto,forma_pago" \
        "${BQ_DATASET}.${BQ_TABLE}" \
        "${SCHEMA_DEFINITION}"        
    echo " ¡Tabla analítica particionada y clusterizada creada!"
else
    echo " La tabla ${BQ_DATASET}.${BQ_TABLE} ya existe. Omitiendo creación."
fi


# 4. CREACIÓN DEL BUCKET DE STAGING PARA DATAFLOW
echo "### [5/7] Configurando Bucket de almacenamiento intermedio..."

if ! gsutil ls -b "gs://${GCS_BUCKET_NAME}" >/dev/null 2>&1; then
    echo " Creando Storage Bucket: gs://${GCS_BUCKET_NAME}..."
    gsutil mb -l "${REGION}" -p "${PROJECT_ID}" "gs://${GCS_BUCKET_NAME}/"
else
    echo " El bucket gs://${GCS_BUCKET_NAME} ya existe."
fi

# 5. VALIDACIÓN / CREACIÓN DE LA SERVICE ACCOUNT PARA WORKERS DE DATAFLOW
echo "### [6/7] Validando permisos y Service Account para Dataflow Workers..."

# 5.1 Verificar si la Service Account ya existe, si no, crearla y asignar roles necesarios
if ! gcloud iam service-accounts list --project="${PROJECT_ID}" --format="value(email)" | grep -q "^${SA_DATAFLOW}$"; then
    echo " Creando SA para Dataflow Workers..."
    gcloud iam service-accounts create "sa-dataflow-worker" \
        --description="Service Account para los workers del pipeline de Dataflow" \
        --display-name="Dataflow Worker Service Account" \
        --project="${PROJECT_ID}"
    
    sleep 10 # Espera de cortesía para propagación de IAMs
    echo " Asignando roles necesarios (Dataflow Worker, Pub/Sub Subscriber, BigQuery DataEditor)..."
    for ROLE in roles/dataflow.worker roles/pubsub.subscriber roles/bigquery.dataEditor roles/storage.objectAdmin; do
        gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
            --member="serviceAccount:${SA_DATAFLOW}" \
            --role="${ROLE}" >/dev/null
    done
else
    echo " [OK] La Service Account ${SA_DATAFLOW} ya está configurada."
fi

# 7. DESPLIEGUE DEL PIPELINE DE DATAFLOW (STREAMING)
echo "### [7/7] Lanzando Job de Dataflow en modo Streaming..."

# Verificar si el Job ya se encuentra corriendo para evitar duplicaciones analíticas
if ! gcloud dataflow jobs list --project="${PROJECT_ID}" --region="${REGION}" --status=active --format="value(name)" | grep -q "^${DATAFLOW_JOB_NAME}$"; then
    
    gcloud dataflow jobs run "${DATAFLOW_JOB_NAME}" \
        --gcs-location="gs://dataflow-templates-${REGION}/latest/PubSub_Subscription_to_BigQuery" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --service-account-email="${SA_DATAFLOW}" \
        --staging-location="gs://${GCS_BUCKET_NAME}/staging/" \
        --parameters="inputSubscription=projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME},outputTableSpec=${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}"

    echo " ¡Job de Dataflow lanzado con éxito!"
else
    echo " WARNING: Ya existe un Job activo con el nombre ${DATAFLOW_JOB_NAME}. Omitiendo despliegue."
fi

echo "========== [DEPLOYMENT SUCCESSFUL - END] =========="