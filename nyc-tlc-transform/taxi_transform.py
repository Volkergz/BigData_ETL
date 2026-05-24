#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType, DateType, DoubleType, TimestampType, StringType

def init_spark():
    """Inicializa la sesión de Spark optimizada para Dataproc Serverless."""
    spark = SparkSession.builder \
        .appName("NYC-TLC-Yellow-Taxi-ETL") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    return spark

def parse_arguments():
    parser = argparse.ArgumentParser(description="ETL Pipeline para Yellow Taxi usando Dataproc Serverless")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--dataset_id", required=True, help="BigQuery Dataset ID")
    parser.add_argument("--bucket_name", required=True, help="GCS Bucket Name de entrada/salida")
    return parser.parse_args()

def generate_surrogate_key_hash(col_list):
    concat_col = F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in col_list])
    return F.conv(F.substring(F.sha2(concat_col, 256), 1, 15), 16, 10).cast(LongType())

def main():
    args = parse_arguments()
    spark = init_spark()
    
    input_path = f"gs://{args.bucket_name}/raw/nyc_tlc/yellow/*.parquet"
    print(f"[*] Leyendo archivos origen Parquet desde: {input_path}")
    
    # 1. LECTURA (Inferencia automática para evitar errores de diccionario Parquet)
    df_raw = spark.read.parquet(input_path)

    # ============================================================================
    # 2. POST-LECTURA: Normalización y Limpieza de Esquema (Dinámica)
    # ============================================================================
    expected_cols = [
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "passenger_count", "trip_distance", "ratecodeid",
        "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", 
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
        "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
    ]
    
    current_cols = [c.lower() for c in df_raw.columns]
    
    # Seleccionamos columnas existentes o las creamos como null si faltan en algún archivo
    df_normalized = df_raw.select([
        F.col(c).alias(c.lower()) if c.lower() in current_cols 
        else F.lit(None).alias(c.lower()) 
        for c in expected_cols
    ])

    # 3. CASTING Y LIMPIEZA
    df_cast = df_normalized.withColumn("vendorid", F.col("vendorid").cast(LongType())) \
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType())) \
        .withColumn("passenger_count", F.col("passenger_count").cast(DoubleType())) \
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType())) \
        .withColumn("ratecodeid", F.col("ratecodeid").cast(DoubleType())) \
        .withColumn("pulocationid", F.col("pulocationid").cast(LongType())) \
        .withColumn("dolocationid", F.col("dolocationid").cast(LongType())) \
        .withColumn("payment_type", F.col("payment_type").cast(LongType())) \
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType())) \
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))

    df_cleaned = df_cast.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
    )

    # 4. DIMENSIONES Y HECHOS (Usa las columnas en minúsculas)
    # ... (El resto de tu lógica de dimensiones permanece igual, asegurando usar F.col("vendorid") etc.)
    
    # Ejemplo ajuste en Fact Trips:
    df_fact_trips = df_cleaned.withColumn(
        "trip_id_pk", generate_surrogate_key_hash(["vendorid", "tpep_pickup_datetime", "pulocationid", "dolocationid"])
    ).select(
        F.col("trip_id_pk"),
        F.col("vendorid").alias("vendor_id_fk"),
        # ... resto de columnas
    )

    # 5. ESCRITURA EN BIGQUERY
    # (Mantén tu lógica de bucle para write mode)
    
    print("[+] ETL procesado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    main()