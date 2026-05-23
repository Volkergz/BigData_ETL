#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DateType

def init_spark():
    """Inicializa la sesión de Spark optimizada para Dataproc Serverless."""
    spark = SparkSession.builder \
        .appName("NYC-TLC-Yellow-Taxi-ETL") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    return spark

def parse_arguments():
    """Parsea los argumentos enviados desde el script de Bash."""
    parser = argparse.ArgumentParser(description="ETL Pipeline para Yellow Taxi usando Dataproc Serverless")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--dataset_id", required=True, help="BigQuery Dataset ID")
    parser.add_argument("--bucket_name", required=True, help="GCS Bucket Name de entrada/salida")
    return parser.parse_args()

def generate_surrogate_key_hash(col_list):
    """Genera una llave subrogada numérica INT64 (BigQuery) determinista basada en hash."""
    # Concatenamos las columnas con un separador e implementamos SHA-2
    concat_col = F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in col_list])
    # Tomamos los primeros 15 caracteres del hash hexadecimal y los convertimos a un entero largo (INT64 de BQ)
    return F.conv(F.substring(F.sha2(concat_col, 256), 1, 15), 16, 10).cast(LongType())

def main():
    args = parse_arguments()
    spark = init_spark()
    
    # Rutas de almacenamiento en GCS (Asumiendo estructura estándar de ingesta)
    # Reemplaza 'raw/*.parquet' por tu patrón de archivos específico en GCS
    input_path = f"gs://{args.bucket_name}/raw/nyc_tlc/yellow/*.parquet"
    
    print(f"[*] Leyendo archivos origen Parquet desde: {input_path}")
    df_raw = spark.read.parquet(input_path)
    
    # ============================================================================
    # 1. LIMPIEZA Y CALIDAD DE DATOS (DATA CLEANSING)
    # ============================================================================
    # Filtrar registros inconsistentes habituales en el dataset de TLC NY
    df_cleaned = df_raw.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")) &
        (F.col("passenger_count") >= 0) &
        (F.col("trip_distance") >= 0.0)
    )

    # ============================================================================
    # 2. GENERACIÓN DINÁMICA DE DIMENSIONES ESTÁTICAS (SFT / ESCALABILIDAD)
    # ============================================================================
    # Dim_Vendor 
    vendor_data = [(1, "Creative Mobile Technologies, LLC"), (2, "VeriFone Inc.")]
    df_dim_vendor = spark.createDataFrame(vendor_data, ["vendor_id_pk", "vendor_name"])
    
    # Dim_Payment_Type 
    payment_data = [
        (1, "Credit card"), (2, "Cash"), (3, "No charge"), 
        (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")
    ]
    df_dim_payment = spark.createDataFrame(payment_data, ["payment_type_id_pk", "payment_description"])
    
    # Dim_Rate_Code 
    rate_data = [
        (1, "Standard rate"), (2, "JFK"), (3, "Newark"), 
        (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride")
    ]
    df_dim_rate = spark.createDataFrame(rate_data, ["rate_code_id_pk", "rate_code_description"])
    
    # Dim_Store_and_fwd_flag 
    flag_data = [(1, "Y", "store and forward trip"), (2, "N", "not a store and forward trip")]
    df_dim_flag = spark.createDataFrame(flag_data, ["flag_id_pk", "flag_code", "flag_desc"])

    # ============================================================================
    # 3. CONSTRUCCIÓN DE DIMENSIONES DE ALTA CARDINALIDAD Y TIEMPO
    # ============================================================================
    
    # --- DIM_LOCATION (Extracción única de coordenadas origen y destino) ---
    df_pickups = df_cleaned.select(
        F.col("Pickup_longitude").alias("longitude"), 
        F.col("Pickup_latitude").alias("latitude")
    ).distinct()
    
    df_dropoffs = df_cleaned.select(
        F.col("Dropoff_longitude").alias("longitude"), 
        F.col("Dropoff_latitude").alias("latitude")
    ).distinct()
    
    df_dim_location_raw = df_pickups.union(df_dropoffs).distinct().filter(
        (F.col("longitude") != 0.0) & (F.col("latitude") != 0.0)
    )
    
    df_dim_location = df_dim_location_raw.withColumn(
        "location_id_pk", generate_surrogate_key_hash(["latitude", "longitude"])
    ).withColumn(
        "latitude", F.col("latitude").cast(DecimalType(18, 9))
    ).withColumn(
        "longitude", F.col("longitude").cast(DecimalType(18, 9))
    ).withColumn(
        "zone_id_fk", F.lit(None).cast(LongType()) # Dejar listo para mapeo posterior con zonas TLC si se requiere
    ).select("location_id_pk", "latitude", "longitude", "zone_id_fk")

    # --- DIM_TIME (Extracción y parseo de marcas de tiempo) ---
    df_times_raw = df_cleaned.select(F.col("tpep_pickup_datetime").alias("ts")).union(
        df_cleaned.select(F.col("tpep_dropoff_datetime").alias("ts"))
    ).distinct()
    
    df_dim_time = df_times_raw.withColumn(
        "time_id_pk", F.date_format(F.col("ts"), "yyyyMMdd").cast(LongType())
    ).withColumn(
        "full_date", F.col("ts")
    ).withColumn(
        "year", F.year(F.col("ts"))
    ).withColumn(
        "month", F.month(F.col("ts"))
    ).withColumn(
        "day", F.dayofmonth(F.col("ts"))
    ).withColumn(
        "hour", F.hour(F.col("ts"))
    ).withColumn(
        "minute", F.minute(F.col("ts"))
    ).withColumn(
        "second", F.second(F.col("ts"))
    ).distinct()

    # ============================================================================
    # 4. CONSTRUCCIÓN DE LA TABLA DE HECHOS (FACT_TRIPS)
    # ============================================================================
    
    # Preparación de llaves foráneas y mapeos numéricos
    df_fact_prep = df_cleaned.withColumn(
        "trip_id_pk", generate_surrogate_key_hash(["VendorID", "tpep_pickup_datetime", "Dropoff_longitude", "Dropoff_latitude"])
    ).withColumn(
        "vendor_id_fk", F.col("VendorID").cast(LongType())
    ).withColumn(
        "rate_code_id_fk", F.col("RateCodeID").cast(LongType())
    ).withColumn(
        "payment_type_id_fk", F.col("Payment_type").cast(LongType())
    ).withColumn(
        "pickup_location_id_fk", generate_surrogate_key_hash(["Pickup_latitude", "Pickup_longitude"])
    ).withColumn(
        "dropoff_location_id_fk", generate_surrogate_key_hash(["Dropoff_latitude", "Dropoff_longitude"])
    ).withColumn(
        "flag_id_fk", F.when(F.col("Store_and_fwd_flag") == "Y", F.lit(1)).otherwise(F.lit(2)).cast(LongType())
    ).withColumn(
        "pickup_date_id_fk", F.date_format(F.col("tpep_pickup_datetime"), "yyyyMMdd").cast(LongType())
    ).withColumn(
        "dropoff_date_id_fk", F.date_format(F.col("tpep_dropoff_datetime"), "yyyyMMdd").cast(LongType())
    ).withColumn(
        "dataflow_partition_date", F.col("tpep_pickup_datetime").cast(DateType())
    )
    
    # Selección final con casts rigurosos según DDL de BigQuery
    df_fact_trips = df_fact_prep.select(
        F.col("trip_id_pk"),
        F.col("vendor_id_fk"),
        F.col("rate_code_id_fk"),
        F.col("payment_type_id_fk"),
        F.col("pickup_location_id_fk"),
        F.col("dropoff_location_id_fk"),
        F.col("flag_id_fk"),
        F.col("pickup_date_id_fk"),
        F.col("dropoff_date_id_fk"),
        F.col("passenger_count").cast(LongType()),
        F.col("trip_distance").cast(DecimalType(18, 4)),
        F.col("fare_amount").cast(DecimalType(18, 4)),
        F.col("extra").cast(DecimalType(18, 4)),
        F.col("mta_tax").cast(DecimalType(18, 4)),
        F.col("improvement_surcharge").cast(DecimalType(18, 4)),
        F.col("tip_amount").cast(DecimalType(18, 4)),
        F.col("tolls_amount").cast(DecimalType(18, 4)),
        F.col("total_amount").cast(DecimalType(18, 4)),
        F.col("dataflow_partition_date")
    )

    # ============================================================================
    # 5. ESCRITURA EN BIGQUERY (USANDO EL CONECTOR OFICIAL AVANZADO)
    # ============================================================================
    # Definición de configuraciones de destino
    bq_dataset = f"{args.project_id}.{args.dataset_id}"
    
    # Diccionario de tablas a guardar (Optimizado para iteración limpia)
    tables_to_write = {
        "Dim_Vendor": df_dim_vendor,
        "Dim_Payment_Type": df_dim_payment,
        "Dim_Rate_Code": df_dim_rate,
        "Dim_Store_and_fwd_flag": df_dim_flag,
        "Dim_Location": df_dim_location,
        "Dim_Time": df_dim_time,
        "Fact_Trips": df_fact_trips
    }
    
    for table_name, df_target in tables_to_write.items():
        full_table_path = f"{bq_dataset}.{table_name}"
        print(f"[*] Cargando datos en BigQuery: {full_table_path}...")
        
        # El conector infiere las particiones automáticamente si la tabla ya está creada
        df_target.write \
            .format("bigquery") \
            .option("table", full_table_path) \
            .option("temporaryGcsBucket", args.bucket_name) \
            .mode("append") \
            .save()
            
    print("[+] ETL procesado e insertado exitosamente de punta a punta.")
    spark.stop()

if __name__ == "__main__":
    main()