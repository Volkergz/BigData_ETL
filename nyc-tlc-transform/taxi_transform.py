#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DateType, DoubleType, TimestampType

def init_spark():
    """Inicializa la sesión de Spark optimizada para Dataproc Serverless."""
    spark = SparkSession.builder \
        .appName("NYC-TLC-Yellow-Taxi-ETL") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
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
    """Genera una llave subrogada numérica INT64 determinista basada en hash para IDs técnicos."""
    concat_col = F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in col_list])
    return F.conv(F.substring(F.sha2(concat_col, 256), 1, 15), 16, 10).cast(LongType())

def main():
    args = parse_arguments()
    spark = init_spark()
    
    input_path = f"gs://{args.bucket_name}/raw/nyc_tlc/yellow/*.parquet"
    print(f"[*] Leyendo archivos origen Parquet con Estrategia Defensiva (Blind String Read) desde: {input_path}")
    
    # ============================================================================
    # 0. EXTRACCIÓN DEFENSIVA (Bypass de ClassCastException binario)
    # ============================================================================
    # Definimos absolutamente todo como String para que el decoder de Parquet 
    # convierta los Double/Long conflictivos a texto plano sin fallar.
    all_columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", 
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
        "improvement_surcharge", "total_amount", "congestion_surcharge", 
        "airport_fee"
    ]
    
    string_schema = StructType([StructField(c, StringType(), True) for c in all_columns])
    df_raw = spark.read.schema(string_schema).parquet(input_path)

    # ============================================================================
    # 1. PARSEO Y CONSOLIDACIÓN DE TIPOS (Casting Seguro Post-Lectura)
    # ============================================================================
    df_cast = df_raw.withColumn("VendorID", F.col("VendorID").cast(LongType())) \
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType())) \
        .withColumn("passenger_count", F.col("passenger_count").cast(DoubleType())) \
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType())) \
        .withColumn("RatecodeID", F.coalesce(F.col("RatecodeID"), F.col("RateCodeID")).cast(DoubleType())) \
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType())) \
        .withColumn("PULocationID", F.col("PULocationID").cast(LongType())) \
        .withColumn("DOLocationID", F.col("DOLocationID").cast(LongType())) \
        .withColumn("payment_type", F.col("payment_type").cast(LongType())) \
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType())) \
        .withColumn("extra", F.col("extra").cast(DoubleType())) \
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType())) \
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType())) \
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType())) \
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType())) \
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType())) \
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType())) \
        .withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))

    # ============================================================================
    # 2. LIMPIEZA Y CALIDAD DE DATOS (DATA CLEANSING)
    # ============================================================================
    df_cleaned = df_cast.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")) &
        (F.col("passenger_count") >= 0) &
        (F.col("trip_distance") >= 0.0)
    )

    # ============================================================================
    # 3. DIMENSIONES ESTÁTICAS
    # ============================================================================
    vendor_data = [(1, "Creative Mobile Technologies, LLC"), (2, "VeriFone Inc.")]
    df_dim_vendor = spark.createDataFrame(vendor_data, ["vendor_id_pk", "vendor_name"])
    
    payment_data = [
        (1, "Credit card"), (2, "Cash"), (3, "No charge"), 
        (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")
    ]
    df_dim_payment = spark.createDataFrame(payment_data, ["payment_type_id_pk", "payment_description"])
    
    rate_data = [
        (1, "Standard rate"), (2, "JFK"), (3, "Newark"), 
        (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride")
    ]
    df_dim_rate = spark.createDataFrame(rate_data, ["rate_code_id_pk", "rate_code_description"])
    
    flag_data = [(1, "store and forward trip"), (2, "not a store and forward trip")]
    df_dim_flag = spark.createDataFrame(flag_data, ["flag_id_pk", "flag_description"])

    # ============================================================================
    # 4. DIMENSIONES DINÁMICAS
    # ============================================================================
    df_puzones = df_cleaned.select(F.col("PULocationID").alias("zone_id")).distinct()
    df_dozones = df_cleaned.select(F.col("DOLocationID").alias("zone_id")).distinct()
    df_dim_location_raw = df_puzones.union(df_dozones).distinct().filter(F.col("zone_id").isNotNull())
    
    df_dim_location = df_dim_location_raw.withColumn(
        "location_id_pk", F.col("zone_id").cast(LongType())
    ).withColumn(
        "latitude", F.lit(None).cast(DecimalType(18, 9))
    ).withColumn(
        "longitude", F.lit(None).cast(DecimalType(18, 9))
    ).withColumn(
        "zone_id_fk", F.col("zone_id").cast(LongType())
    ).select("location_id_pk", "latitude", "longitude", "zone_id_fk")

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
    # 5. TABLA DE HECHOS
    # ============================================================================
    df_fact_prep = df_cleaned.withColumn(
        "trip_id_pk", generate_surrogate_key_hash(["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"])
    ).withColumn(
        "vendor_id_fk", F.col("VendorID").cast(LongType())
    ).withColumn(
        "rate_code_id_fk", F.col("RatecodeID").cast(LongType())
    ).withColumn(
        "payment_type_id_fk", F.col("payment_type").cast(LongType())
    ).withColumn(
        "pickup_location_id_fk", F.col("PULocationID").cast(LongType())
    ).withColumn(
        "dropoff_location_id_fk", F.col("DOLocationID").cast(LongType())
    ).withColumn(
        "flag_id_fk", F.when(F.col("store_and_fwd_flag") == "Y", F.lit(1)).otherwise(F.lit(2)).cast(LongType())
    ).withColumn(
        "pickup_date_id_fk", F.date_format(F.col("tpep_pickup_datetime"), "yyyyMMdd").cast(LongType())
    ).withColumn(
        "dropoff_date_id_fk", F.date_format(F.col("tpep_dropoff_datetime"), "yyyyMMdd").cast(LongType())
    ).withColumn(
        "dataflow_partition_date", F.col("tpep_pickup_datetime").cast(DateType())
    )
    
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
    # 6. ESCRITURA EN BIGQUERY
    # ============================================================================
    bq_dataset = f"{args.project_id}.{args.dataset_id}"
    
    tables_to_write = {
        "Dim_Vendor": (df_dim_vendor, "overwrite"),
        "Dim_Payment_Type": (df_dim_payment, "overwrite"),
        "Dim_Rate_Code": (df_dim_rate, "overwrite"),
        "Dim_Store_and_fwd_flag": (df_dim_flag, "overwrite"),
        "Dim_Location": (df_dim_location, "overwrite"),
        "Dim_Time": (df_dim_time, "overwrite"),
        "Fact_Trips": (df_fact_trips, "append")
    }
    
    for table_name, (df_target, write_mode) in tables_to_write.items():
        full_table_path = f"{bq_dataset}.{table_name}"
        print(f"[*] Cargando datos en BigQuery ({write_mode}): {full_table_path}...")
        
        df_target.write \
            .format("bigquery") \
            .option("table", full_table_path) \
            .option("temporaryGcsBucket", args.bucket_name) \
            .mode(write_mode) \
            .save()
            
    print("[+] ETL procesado e insertado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    main()