#!/bin/bash

# Exit inmediatamente si un comando falla
set -e

echo "=== [1/4] CONFIGURANDO VARIABLES DE ENTORNO ==="
export PROJECT_ID=$(gcloud config get-value project)
export DATASET_NAME="nyc_analytics"
export LOCATION="US"
export SQL_FILE="taxis_dimensional.sql"

echo "Proyecto Actual: ${PROJECT_ID}"

echo ""
echo "=== [2/4] CREANDO EL DATASET EN LA UBICACIÓN 'US' ==="
bq --location=$LOCATION mk --dataset "${PROJECT_ID}:${DATASET_NAME}"

echo "DataSet ${DATASET_NAME} Creado con exito"

echo ""
echo "=== [3/4] EJECUTANDO SENTENCIAS DDL DESDE EL ARCHIVO $SQL_FILE..."
echo "Ejecutando sentencias DDL desde el archivo $SQL_FILE..."

if [ ! -f "$SQL_FILE" ]; then
    echo "ERROR: El archivo '$SQL_FILE' no existe en el directorio actual."
    exit 1
fi

# --use_legacy_sql=false: Fuerza el uso de Standard SQL (necesario para las PK/FK)
# --default_dataset: Inyecta el contexto del dataset para no usar prefijos en el SQL
bq query \
  --use_legacy_sql=false \
  --location=$LOCATION \
  --dataset_id="$PROJECT_ID:$DATASET_NAME" \
  < "$SQL_FILE"