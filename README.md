# BigData_ETL - Guia de Ejecución

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

## Parte 1 - Extracción

1. Ejecutamos el script que ejecuta la extración

```
.nyc-tlc-extractor/deploy.sh
```

> Antes de continuar, espera que el script termine su ejecución

2. Activamos manualmente el trigger del servicio

```
gcloud scheduler jobs run nyc-tlc-monthly-sync --location={Añade aqui la región}
```

> Ahora el **Job** esta descargando todos los archivos, el proceso puede tardar hasta 10min en completarse

## Parte 2 - Carga

> En esta sección **Solo se realizara la configuración inicial** que debe tener BigQuery para poder guardar la data procesada

1. Ejecutamos el script que ejecuta configuración del ambiente

```
./nyc-tlc-loader/loading.sh
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