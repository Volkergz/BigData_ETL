import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, hash, when

def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--dataset_id', required=True)
    parser.add_argument('--bucket_name', required=True)
    args = parser.parse_args()

    # Inicializar sesión Spark
    spark = SparkSession.builder.appName("Taxi_Serverless_ETL").getOrCreate()

    # 1. LECTURA TOLERANTE A FALLOS
    # mergeSchema=true unifica automáticamente las discrepancias entre int64 y float64
    input_path = f"gs://{args.bucket_name}/raw/*.parquet"
    df_raw = spark.read.option("mergeSchema", "true").parquet(input_path)

    # 2. TRANSFORMACIÓN Y LIMPIEZA
    # Casteo estricto para encajar en el modelo dimensional de BQ
    df_transformed = df_raw \
        .withColumn("vendor_id_fk", col("VendorID").cast("long")) \
        .withColumn("rate_code_id_fk", col("RateCodeID").cast("long")) \
        .withColumn("payment_type_id_fk", col("Payment_type").cast("long")) \
        .withColumn("passenger_count", col("Passenger_count").cast("long")) \
        .withColumn("trip_distance", col("Trip_distance").cast("decimal(7,2)")) \
        .withColumn("fare_amount", col("Fare_amount").cast("decimal(8,2)")) \
        .withColumn("extra", col("Extra").cast("decimal(6,2)")) \
        .withColumn("mta_tax", col("MTA_tax").cast("decimal(4,2)")) \
        .withColumn("improvement_surcharge", col("Improvement_surcharge").cast("decimal(4,2)")) \
        .withColumn("tip_amount", col("Tip_amount").cast("decimal(7,2)")) \
        .withColumn("tolls_amount", col("Tolls_amount").cast("decimal(6,2)")) \
        .withColumn("total_amount", col("Total_amount").cast("decimal(9,2)")) \
        .withColumn("flag_id_fk", when(col("Store_and_fwd_flag") == "Y", 1).otherwise(2).cast("long")) \
        .withColumn("pickup_date_id_fk", date_format(col("tpep_pickup_datetime"), "yyyyMMdd").cast("long")) \
        .withColumn("dropoff_date_id_fk", date_format(col("tpep_dropoff_datetime"), "yyyyMMdd").cast("long")) \
        .withColumn("pickup_location_id_fk", hash(col("Pickup_longitude"), col("Pickup_latitude"))) \
        .withColumn("dropoff_location_id_fk", hash(col("Dropoff_longitude"), col("Dropoff_latitude"))) \
        .withColumn("dataflow_partition_date", to_date(col("tpep_pickup_datetime")))

    # 3. SELECCIÓN DE COLUMNAS (Mismo orden y nombre que la tabla Fact_Trips)
    df_final = df_transformed.select(
        "vendor_id_fk", "rate_code_id_fk", "payment_type_id_fk", 
        "pickup_location_id_fk", "dropoff_location_id_fk", "flag_id_fk", 
        "pickup_date_id_fk", "dropoff_date_id_fk", "passenger_count", 
        "trip_distance", "fare_amount", "extra", "mta_tax", 
        "improvement_surcharge", "tip_amount", "tolls_amount", 
        "total_amount", "dataflow_partition_date"
    )

    # 4. CARGA A BIGQUERY
    # temporaryGcsBucket es requerido por el conector de Spark-BigQuery
    table_ref = f"{args.project_id}.{args.dataset_id}.Fact_Trips"
    
    df_final.write.format("bigquery") \
        .option("table", table_ref) \
        .option("temporaryGcsBucket", args.bucket_name) \
        .mode("append") \
        .save()

if __name__ == "__main__":
    run_pipeline()