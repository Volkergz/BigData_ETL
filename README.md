# BigData_ETL - Guia de Ejecución BATCH

## Configuración Inicial

1. Clona el repositorio en CloudShell

```
git clone https://github.com/Volkergz/BigData_ETL.git
```

2. Cambiamos al directorio del proyecto

```
cd ./BigData_ETL
```

3. Otorgamos permiso de ejecusión a los archivos

```
chmod +x ./*/*.sh
```

## 1. Función: de .parquet a BigQuery

> Esta función es activada cuando se crea un nuevo archivo en el bucket "ingesta-nyc-tlc", en donde toma el nuevo archivo, crea una tabla en BigQuery con el nombre del archivo, y luego guarda todos los datos en dicha tabla. Este proceso lo repite por cada uno de los archivos .parquet.

1. Ejecutamos el script que ejecuta la extración
```
./func-parquet-to-BQ/script.sh
```


## 2. Función: descarga de .parquet

> Esta función es activada al princio de cada vez (o Manualmente), en donde primero, revisa en el bucket los archivos ya existentes, y en base a ellos, genera una lista de fechas para descargar los archivos, posteriormente solicita los recursos a travez de una petición https a NYC-TLC para cara archivo y los guardar en el Bucket.

1. Ejecutamos el script que ejecuta la extración
```
./func-download-parquet/script.sh
```

> Antes de continuar, espera que el script termine su ejecución

2. Activamos manualmente el trigger del servicio
```
gcloud scheduler jobs run nyc-tlc-monthly-sync --location={Añade aqui la región}
```

> Ahora el **Job** esta descargando todos los archivos, el proceso puede tardar hasta 10min en completarse

# ¡TRABAJO EN PROCESO!

## Función: crear base de datos dimensional

> En esta sección **Solo se realizara la creación de las tablas** que debe tener BigQuery para poder guardar la data procesada.

1. Ejecutamos el script que ejecuta configuración del ambiente
```
./func-create-DB/script.sh
```

> Antes de continuar, espera que el script termine su ejecución

## Parte 3 - Transformación

1. Habilitamos el permiso de ejecutar los servicios en redes privadas

```
gcloud compute networks subnets update default \
  --region={Añade aqui la región} \
  --enable-private-ip-google-access
```

2. Ejecutamos el script que inicializa el proceso de transformación  en dataproc

```
./nyc-tlc-tranform/transformation.sh
```

> Ahora solo esperamos que los datos sean procesados, este proceso puede tardar mucho tiempo.

# BigData_ETL - Guia de Ejecución STREAMING

## Configuración Inicial

1. Clona el repositorio en CloudShell

```
git clone --single-branch --branch streaming https://github.com/Volkergz/BigData_ETL.git
```

2. Cambiamos al directorio del proyecto

```
cd ./BigData_ETL
```

3. Otorgamos permiso de ejecusión a los archivos

```
chmod +x ./Streaming/*/*.sh
```

## 1. Función: creación del webhook (Pub/Sub)

> Esta función crear el Pipeline que recibe los mensajes del servidor con los datos de compra, desplegado en una cloud Function, utilizando Pub/Sub.

1. Ejecutamos el script que ejecuta la creación del Pipeline
```
./Streaming/func-create-webhook/script.sh
```

> Una vez creada la cloud function, ve a la sección **Cloud Run**, y en la función que acabamos de crear, encontrataras el enlace https que debes copiar.

2. Para acceder al servidor que nos proveera los registros, debemos entrar en el siguiente [enlance](https://bdrealtimeescuelait.duoc.cl/login/).

3. Una vez hayamos hecho login en el sitio web, vamos a ingresar en el campo **URL** la direción que obtuvimos en __Cloud Run__.

## 2. Función: creación del webhook (Pub/Sub)

> Esta función crear el Pipeline que recibe los archivos JSON y los guarda en BigQuery.

1. Ejecutamos el script que ejecuta la creación del Pipeline
```
./Streaming/func-pubsub-to-BQ/script.sh
```
