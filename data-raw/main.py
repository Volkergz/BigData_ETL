import functions_framework
import re
import os
import base64
import json
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT")
DATASET_ID = os.getenv("DATASET_ID", "nyc_tlc_yellow")
LOCATION = os.getenv("DATASET_LOCATION", "US")

bq_client = bigquery.Client(project=PROJECT_ID)

def sanitize_table_name(filename: str) -> str:
    name_without_ext = filename.split('/')[-1].replace('.parquet', '')
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name_without_ext)
    if re.match(r'^[0-9]', sanitized):
        sanitized = f"t_{sanitized}"
    return sanitized

@functions_framework.http
def gcs_to_bigquery_trigger(request):
    # Validar que el Healthcheck de Cloud Run pase sin problemas
    if request.method == "GET":
        return "OK", 200

    request_json = request.get_json(silent=True)
    
    # 1. PARSEO SEGURO DEL payload DE PUB/SUB PUSH
    if not request_json or "message" not in request_json:
        print("Error: Payload HTTP no contiene el nodo 'message' de Pub/Sub.")
        return "Bad Request: Missing Pub/Sub message structure", 400
        
    pubsub_message = request_json["message"]
    if "data" not in pubsub_message:
        print("Error: El mensaje de Pub/Sub viene vacío ('data' absent).")
        return "Bad Request: Missing data inside Pub/Sub message", 400
        
    try:
        # Pub/Sub emite los datos codificados en Base64 estándar
        raw_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        gcs_event_data = json.loads(raw_data)
        
        bucket_name = gcs_event_data.get("bucket")
        blob_name = gcs_event_data.get("name")
    except Exception as parse_err:
        print(f"Error crítico decodificando el Base64 de GCS: {str(parse_err)}")
        return "Internal Error: JSON Payload corrupt", 500
    
    # 2. FILTRADO POR PREFIJO
    TARGET_PREFIX = "raw/nyc_tlc/yellow/"
    if not blob_name or not blob_name.startswith(TARGET_PREFIX) or not blob_name.endswith('.parquet'):
        print(f"File ignorado (No pertenece a la ruta analítica): {blob_name}")
        return "Ignored by prefix/extension rules", 200

    print(f"Iniciando ingesta analítica para: gs://{bucket_name}/{blob_name}")
    
    # 3. VERIFICACIÓN E INGESTA EN BIGQUERY
    try:
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        bq_client.create_dataset(dataset, exists_ok=True)
        
        table_id = sanitize_table_name(blob_name)
        table_ref = dataset_ref.table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="tpep_pickup_datetime"
            ),
            clustering_fields=["PULocationID", "DOLocationID"]
        )
        
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result() # Esperar el commit en BigQuery
        
        print(f"Éxito rotundo: Tabla {table_id} persistida en BigQuery.")
        return "Success", 200
        
    except Exception as bq_err:
        print(f"Error ejecutando el Load Job en BigQuery: {str(bq_err)}")
        return f"BigQuery Internal Error: {str(bq_err)}", 500