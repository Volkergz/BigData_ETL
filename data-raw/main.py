import functions_framework
import re
import os
from google.cloud import bigquery

# Inicialización global de clientes para reutilizar sockets en invocaciones concurrentes
PROJECT_ID = os.getenv("GCP_PROJECT")
DATASET_ID = os.getenv("DATASET_ID", "nyc_tlc_yellow")
LOCATION = os.getenv("DATASET_LOCATION", "US")

bq_client = bigquery.Client(project=PROJECT_ID)

def sanitize_table_name(filename: str) -> str:
    """Extrae y sanitiza el nombre de la tabla a partir del path del objeto."""
    name_without_ext = filename.split('/')[-1].replace('.parquet', '')
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name_without_ext)
    if re.match(r'^[0-9]', sanitized):
        sanitized = f"t_{sanitized}"
    return sanitized

@functions_framework.cloud_event
def gcs_to_bigquery_trigger(cloud_event):
    """
    Cloud Function activada por eventos de GCS (Cloud Events v2).
    """
    data = cloud_event.data
    bucket_name = data["bucket"]
    blob_name = data["name"]
    
    # Restringir la ejecución exclusivamente al prefijo objetivo de taxis amarillos
    TARGET_PREFIX = "raw/nyc_tlc/yellow/"
    if not blob_name.startswith(TARGET_PREFIX) or not blob_name.endswith('.parquet'):
        print(f"Objeto ignorado (No cumple con el prefijo o extensión): {blob_name}")
        return

    print(f"Procesando archivo detectado: gs://{bucket_name}/{blob_name}")
    
    # 1. Garantizar existencia del Dataset
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    bq_client.create_dataset(dataset, exists_ok=True)
    
    # 2. Configurar la carga a la tabla individual
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
    
    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        load_job.result() # Esperar la ejecución del Job en BigQuery
        
        destination_table = bq_client.get_table(table_ref)
        print(f"Éxito: Tabla {table_id} procesada con {destination_table.num_rows} filas.")
        
    except Exception as e:
        print(f"Error crítico procesando {gcs_uri} hacia BigQuery: {str(e)}")
        raise e