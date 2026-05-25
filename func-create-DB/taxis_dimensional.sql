-- ============================================================================
-- DIMENSIONES
-- ============================================================================

CREATE OR REPLACE TABLE `Dim_Zone_City` (
  zone_id_pk INT64 OPTIONS(description="ID generado mediante hash o proceso ETL"),
  district STRING OPTIONS(description="Ej. Manhattan, Brooklyn, Queens, Bronx, Staten Island"),
  specific_zone STRING OPTIONS(description="Nombre del barrio o zona TLC"),
  PRIMARY KEY (zone_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Vendor` (
  vendor_id_pk INT64 OPTIONS(description="1 = Creative Mobile Technologies, LLC; 2 = VeriFone Inc."), 
  vendor_name STRING,
  PRIMARY KEY (vendor_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Payment_Type` (
  payment_type_id_pk INT64 OPTIONS(description="1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided"), 
  payment_description STRING,
  PRIMARY KEY (payment_type_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Rate_Code` (
  rate_code_id_pk INT64 OPTIONS(description="1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group"), 
  rate_code_description STRING,
  PRIMARY KEY (rate_code_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Location` (
  location_id_pk INT64 OPTIONS(description="ID único generado por hash de coordenadas"),
  latitude NUMERIC OPTIONS(description="Coordenada de Latitud"),
  longitude NUMERIC OPTIONS(description="Coordenada de Longitud"),
  zone_id_fk INT64 OPTIONS(description="Relación con la sub-dimensión de zonas"),
  PRIMARY KEY (location_id_pk) NOT ENFORCED,
  FOREIGN KEY (zone_id_fk) REFERENCES `Dim_Zone_City`(zone_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Time` (
  time_id_pk INT64 OPTIONS(description="Formato numérico inteligente: YYYYMMDD"),
  full_date DATETIME,
  year INT64,
  month INT64,
  day INT64,
  hour INT64,
  minute INT64,
  second INT64,
  PRIMARY KEY (time_id_pk) NOT ENFORCED
);

CREATE OR REPLACE TABLE `Dim_Store_and_fwd_flag` (
  flag_id_pk INT64,
  flag_desc STRING OPTIONS(description="Y = store and forward, N = not a store and forward"), 
  PRIMARY KEY (flag_id_pk) NOT ENFORCED
);

-- ============================================================================
-- TABLA DE HECHOS (FACT TABLE)
-- Optimizada con Particionamiento y Clustering para BigQuery
-- ============================================================================

CREATE OR REPLACE TABLE `Fact_Trips` (
  trip_id_pk INT64 OPTIONS(description="Llave primaria surrogate generada en el ETL"),
  vendor_id_fk INT64 NOT NULL,
  rate_code_id_fk INT64 NOT NULL,
  payment_type_id_fk INT64 NOT NULL,
  pickup_location_id_fk INT64 NOT NULL,
  dropoff_location_id_fk INT64 NOT NULL,
  flag_id_fk INT64,
  pickup_date_id_fk INT64 NOT NULL OPTIONS(description="Conexión a Dim_Time (Origen). Formato YYYYMMDD para emular la partición"),
  dropoff_date_id_fk INT64 NOT NULL OPTIONS(description="Conexión a Dim_Time (Destino)"),
  
  -- Campos nativos de negocio
  passenger_count INT64 OPTIONS(description="Valor ingresado por el conductor"), 
  trip_distance NUMERIC OPTIONS(description="Distancia recorrida en millas"), 
  fare_amount NUMERIC OPTIONS(description="Tarifa calculada por el taxímetro por tiempo/distancia"), 
  extra NUMERIC OPTIONS(description="Extras/recargos (Ej. $0.50 u $1 por hora pico/nocturno)"), 
  mta_tax NUMERIC OPTIONS(description="Impuesto automático de la MTA ($0.50)"), 
  improvement_surcharge NUMERIC OPTIONS(description="Surcharge de mejora de $0.30 (iniciado en 2015)"), 
  tip_amount NUMERIC OPTIONS(description="Poblado automáticamente para tarjetas de crédito"), 
  tolls_amount NUMERIC OPTIONS(description="Total de peajes pagados en el viaje"), 
  total_amount NUMERIC OPTIONS(description="Total cobrado al pasajero (no incluye propinas en efectivo)"), 
    
  -- Campo técnico agregado para habilitar la potencia de particionamiento nativo de BQ
  dataflow_partition_date DATE NOT NULL OPTIONS(description="Campo técnico para particionar físicamente la tabla por fecha de pickup"),

  -- Declaración de restricciones informativas (Muy útil para optimizar Looker)
  PRIMARY KEY (trip_id_pk) NOT ENFORCED,
  FOREIGN KEY (vendor_id_fk) REFERENCES `Dim_Vendor`(vendor_id_pk) NOT ENFORCED,
  FOREIGN KEY (rate_code_id_fk) REFERENCES `Dim_Rate_Code`(rate_code_id_pk) NOT ENFORCED,
  FOREIGN KEY (payment_type_id_fk) REFERENCES `Dim_Payment_Type`(payment_type_id_pk) NOT ENFORCED,
  FOREIGN KEY (pickup_location_id_fk) REFERENCES `Dim_Location`(location_id_pk) NOT ENFORCED,
  FOREIGN KEY (dropoff_location_id_fk) REFERENCES `Dim_Location`(location_id_pk) NOT ENFORCED,
  FOREIGN KEY (pickup_date_id_fk) REFERENCES `Dim_Time`(time_id_pk) NOT ENFORCED,
  FOREIGN KEY (dropoff_date_id_fk) REFERENCES `Dim_Time`(time_id_pk) NOT ENFORCED,
  FOREIGN KEY (flag_id_fk) REFERENCES `Dim_Store_and_fwd_flag`(flag_id_pk) NOT ENFORCED
)
PARTITION BY dataflow_partition_date
CLUSTER BY vendor_id_fk, rate_code_id_fk, payment_type_id_fk;
