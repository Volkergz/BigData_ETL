import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, date_format, hash, when

def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--dataset_id', required=True)
    parser.add_argument('--bucket_name', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Taxi_Serverless_ETL").getOrCreate()

    # SOLUCIÓN CRUCIAL: Desactivar el lector vectorizado para permitir conversiones INT32 -> BIGINT
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    # 1. DEFINICIÓN DEL ESQUEMA DEFENSIVO (Resuelve el conflicto INT vs BIGINT y DOUBLE)
    # Definimos los tipos más permisivos para absorber cualquier variación entre archivos
    custom_schema = StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),      # Resiste INT o FLOAT
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),           # Resiste INT o FLOAT
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])

    # 2. LECTURA CON ESQUEMA INYECTADO (Eliminamos mergeSchema)
    input_path = f"gs://{args.bucket_name}/raw/nyc_tlc/yellow/*.parquet"
    df_raw = spark.read.schema(custom_schema).parquet(input_path)

    # 3. TRANSFORMAR Y ASIGNAR LLAVES PRIMARIAS / FORÁNEAS
    df_transformed = df_raw \
        .withColumn("vendor_id_fk", col("VendorID").cast("long")) \
        .withColumn("rate_code_id_fk", col("RatecodeID").cast("long")) \
        .withColumn("payment_type_id_fk", col("payment_type").cast("long")) \
        .withColumn("passenger_count", col("passenger_count").cast("long")) \
        .withColumn("trip_distance", col("trip_distance").cast("decimal(7,2)")) \
        .withColumn("fare_amount", col("fare_amount").cast("decimal(8,2)")) \
        .withColumn("extra", col("extra").cast("decimal(6,2)")) \
        .withColumn("mta_tax", col("mta_tax").cast("decimal(4,2)")) \
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast("decimal(4,2)")) \
        .withColumn("tip_amount", col("tip_amount").cast("decimal(7,2)")) \
        .withColumn("tolls_amount", col("tolls_amount").cast("decimal(6,2)")) \
        .withColumn("total_amount", col("total_amount").cast("decimal(9,2)")) \
        .withColumn("flag_id_fk", when(col("store_and_fwd_flag") == "Y", 1).otherwise(2).cast("long")) \
        .withColumn("pickup_date_id_fk", date_format(col("tpep_pickup_datetime"), "yyyyMMdd").cast("long")) \
        .withColumn("dropoff_date_id_fk", date_format(col("tpep_dropoff_datetime"), "yyyyMMdd").cast("long")) \
        .withColumn("pickup_location_id_fk", col("PULocationID").cast("long")) \
        .withColumn("dropoff_location_id_fk", col("DOLocationID").cast("long")) \
        .withColumn("dataflow_partition_date", to_date(col("tpep_pickup_datetime")))

    # 4. SELECCIÓN ALINEADA CON EL MODELO EN BIGQUERY
    df_final = df_transformed.select(
        "vendor_id_fk", "rate_code_id_fk", "payment_type_id_fk", 
        "pickup_location_id_fk", "dropoff_location_id_fk", "flag_id_fk", 
        "pickup_date_id_fk", "dropoff_date_id_fk", "passenger_count", 
        "trip_distance", "fare_amount", "extra", "mta_tax", 
        "improvement_surcharge", "tip_amount", "tolls_amount", 
        "total_amount", "dataflow_partition_date"
    )

    # 5. ESCRITURA EN BIGQUERY
    table_ref = f"{args.project_id}.{args.dataset_id}.Fact_Trips"
    
    df_final.write.format("bigquery") \
        .option("table", table_ref) \
        .option("temporaryGcsBucket", args.bucket_name) \
        .mode("append") \
        .save()

if __name__ == "__main__":
    run_pipeline()