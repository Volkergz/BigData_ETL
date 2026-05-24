import os
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import backoff
import pandas as pd
import functions_framework
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("LANDING_BUCKET")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
START_DATE = datetime(2023, 1, 1)


def get_existing_blobs(prefix: str) -> set:
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
        return {os.path.basename(blob.name) for blob in blobs}
    except Exception as e:
        logger.error(f"Error al listar blobs en GCS: {str(e)}")
        raise e


def generate_date_range():
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
def download_and_convert(url: str, parquet_filename: str, csv_blob_path: str) -> bool:
    """Descarga Parquet, convierte a CSV en memoria efímera y sube a GCS."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    tmp_parquet_path = f"/tmp/{parquet_filename}"
    tmp_csv_path = f"/tmp/{parquet_filename.replace('.parquet', '.csv')}"
    
    logger.info(f"Iniciando descarga desde: {url}")
    
    try:
        # 1. Descargar Parquet a disco en memoria (/tmp)
        with requests.get(url, stream=True, timeout=(10, 60)) as r:
            if r.status_code == 404:
                logger.warning(f"Archivo no encontrado en origen (404): {url}")
                return False
            r.raise_for_status()
            with open(tmp_parquet_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        # 2. Conversión a CSV
        logger.info(f"Convirtiendo {parquet_filename} a CSV...")
        df = pd.read_parquet(tmp_parquet_path)
        df.to_csv(tmp_csv_path, index=False)
        
        # 3. Subir CSV a Google Cloud Storage
        blob = bucket.blob(csv_blob_path)
        blob.upload_from_filename(tmp_csv_path, content_type='text/csv')
        logger.info(f"Sincronizado exitosamente: gs://{BUCKET_NAME}/{csv_blob_path}")
        
        return True
        
    finally:
        # 4. Limpieza OBLIGATORIA de RAM
        if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
        if os.path.exists(tmp_csv_path): os.remove(tmp_csv_path)


@functions_framework.http
def sync_nyc_data(request):
    """Punto de entrada HTTP. Almacenamiento aplanado en raw/."""
    data = request.get_json(silent=True) or {}
    v_type = data.get('type', 'yellow')
    
    # Cambio solicitado: prefijo aplanado
    prefix = "raw/"
    
    try:
        existing_files = get_existing_blobs(prefix)
    except Exception:
        return {"status": "error", "message": "Failed to access storage"}, 500
        
    downloaded_count = 0
    
    for year, month in generate_date_range():
        # Mantenemos el nombre de archivo original para identificar el tipo
        csv_filename = f"{v_type}_tripdata_{year}-{month}.csv"
        csv_blob_path = f"{prefix}{csv_filename}"
        
        if csv_filename in existing_files:
            continue
            
        # Generar nombre del parquet original para descargar
        parquet_filename = f"{v_type}_tripdata_{year}-{month}.parquet"
        target_url = f"{BASE_URL}/{parquet_filename}"
        
        if download_and_convert(target_url, parquet_filename, csv_blob_path):
            downloaded_count += 1
            
    return {
        "status": "completed", 
        "target_directory": prefix,
        "new_csv_files_ingested": downloaded_count
    }, 200