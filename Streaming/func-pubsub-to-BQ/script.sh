#!/usr/bin/env bash

# Manejo estricto de errores (Fail fast)
set -euo pipefail
IFS=$'\n\t'

echo "========== [START] Inicializando Pipeline Analítico (Pub/Sub -> BigQuery) =========="

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

# Fallback si no hay org policy restrictiva explícita
if [[ -z "$REGION" ]]; then
    export REGION=$(gcloud config get-value compute/region)
    [[ -z "$REGION" ]] && export REGION="us-central1"
fi

gcloud config set compute/region "$REGION"
echo " - Región configurada: $REGION"

# 2. Configuración de Variables de Entorno
echo "### [2/6] Inicializando variables de entorno..."
export PROJECT_ID=$(gcloud config get-value project)
export TOPIC_NAME="registros_compras"
export SUBSCRIPTION_NAME="${TOPIC_NAME}-bq-direct-sub"

export SA_EMAIL="webhook-registros-compras@${PROJECT_ID}.iam.gserviceaccount.com"
#export SERVICE_NAME="webhook-registros-compras"

# Destino Analítico en BigQuery
export BQ_DATASET="analytics_compras"
export BQ_TABLE="raw_registros_compras"

# Información de contexto para el usuario
echo " Proyecto:    ${PROJECT_ID}"
echo " Región:      ${REGION}"
echo " Suscripción: ${SUBSCRIPTION_NAME}"
echo " Destino BQ:  ${BQ_DATASET}.${BQ_TABLE}"

# 3. HABILITACIÓN DE APIS NECESARIAS PARA EL STACK ANALÍTICO
echo "### [3/6] Verificando y habilitando APIs de Google Cloud..."

# ¿Que APIS requiere para activar este servicio?
gcloud services enable \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    --quiet

echo " [OK] APIs del stack analítico activadas correctamente."


# 3. CREACIÓN DEL DATASET Y TABLA OPTIMIZADA EN BIGQUERY
echo "### [4/6] Configurando almacenamiento optimizado en BigQuery..."

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

# 5. Asignación de Permisos
echo "### [5/6] Configurando seguridad e Identidades (IAM)..."

# 5.1 Verificar si la Service Account ya existe, si no, crearla y asignar roles necesarios
if  gcloud iam service-accounts list --project="${PROJECT_ID}" --format="value(email)" | grep -q "^${SA_EMAIL}$"; then
    echo " Asignando roles necesarios (BigQuery.DataEditor)..."
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/bigquery.dataEditor" >/dev/null
else
    echo " [OK] La Service Account ${SA_EMAIL} ya está configurada."
fi

# 6. DESPLIEGUE DEL PIPELINE DE DATAFLOW (STREAMING)
echo "### [6/6] Creando la suscripción directa de Pub/Sub a BigQuery..." 
if ! gcloud pubsub subscriptions list --project="${PROJECT_ID}" --format="value(name)" | grep -q "${SUBSCRIPTION_NAME}$"; then
    
    echo " Desplegando BigQuery Subscription..."
    gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
        --topic="${TOPIC_NAME}" \
        --project="${PROJECT_ID}" \
        --bigquery-table="${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}" \
        --drop-unknown-fields \
        --ack-deadline=60
        
    echo " ¡Suscripción enlazada con éxito!"
else
    echo " WARNING: La suscripción '${SUBSCRIPTION_NAME}' ya existe. Omitiendo creación."
fi

echo "=============================================================================="
echo " ¡DESPLIEGUE EXITOSO! El pipeline Zero-Code Pub/Sub -> BigQuery está activo."
echo "=============================================================================="