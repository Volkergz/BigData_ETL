import os
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import backoff
import functions_framework
from google.cloud import storage

# Configuración de Logging de producción
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("LANDING_BUCKET")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
START_DATE = datetime(2023, 1, 1)


def get_existing_blobs(prefix: str) -> set:
    """Retorna un conjunto con los nombres de archivos ya existentes en el bucket."""
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
        return {os.path.basename(blob.name) for blob in blobs}
    except Exception as e:
        logger.error(f"Error al listar blobs en GCS: {str(e)}")
        raise e


def generate_date_range():
    """Generador que produce tuplas (año, mes) desde START_DATE hasta el mes actual."""
    current = START_DATE
    end = datetime.now()
    while current <= end:
        yield current.year, f"{current.month:02d}"
        current += relativedelta(months=1)


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.Timeout),
    max_tries=5,
    giveup=lambda e: e.response is not None and e.response.status_code == 404
)
def download_file(url: str, blob_name: str) -> bool:
    """Descarga un archivo vía streaming y lo escribe directamente en GCS."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    logger.info(f"Iniciando descarga desde: {url}")
    
    # Timeout estricto de conexión (10s) y lectura (60s)
    with requests.get(url, stream=True, timeout=(10, 60)) as r:
        if r.status_code == 404:
            logger.warning(f"Archivo no encontrado en origen (404): {url}")
            return False
        r.raise_for_status()
        
        # Upload directo utilizando el stream crudo de requests
        blob.upload_from_file(r.raw, content_type='application/octet-stream')
    logger.info(f"Sincronizado exitosamente en GCS: gs://{BUCKET_NAME}/{blob_name}")
    return True


@functions_framework.http
def sync_nyc_data(request):
    """Punto de entrada HTTP para Cloud Functions / Cloud Run Gen 2."""
    data = request.get_json(silent=True) or {}
    v_type = data.get('type', 'yellow')
    
    prefix = f"raw/nyc_tlc/{v_type}/"
    
    try:
        existing_files = get_existing_blobs(prefix)
    except Exception:
        return {"status": "error", "message": "Failed to access storage zone"}, 500
        
    downloaded_count = 0
    
    for year, month in generate_date_range():
        file_name = f"{v_type}_tripdata_{year}-{month}.parquet"
        blob_path = f"{prefix}{file_name}"
        
        if file_name in existing_files:
            continue
            
        target_url = f"{BASE_URL}/{file_name}"
        if download_file(target_url, blob_path):
            downloaded_count += 1
            
    return {
        "status": "completed", 
        "vehicle_type": v_type, 
        "new_files_ingested": downloaded_count
    }, 200