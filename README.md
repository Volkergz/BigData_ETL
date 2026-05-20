# BigData_ETL - Guia de Ejecución

## Configuración Inicial

1. Clona el repositorio en CloudShell

```
git clone https://github.com/Volkergz/BigData_ETL.git
```

2. Otorgamos permiso de ejecusión a los archivos

```
chmod +x extraction.sh loading.sh transformation.sh
```

## Parte 1 - Extracción

1. Ejecutamos el script que ejecuta la extración

```
./extraction.sh
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
./loading.sh
```

> Antes de continuar, espera que el script termine su ejecución

## Parte 3 - Transformación

1. Habilitamos

```
gcloud compute networks subnets update default \
  --region=us-central1 \
  --enable-private-ip-google-access
```

2. Ejecutamos el script que inicializa el proceso de transformación  en dataproc

```
./transformation.sh
```

> Ahora solo esperamos que los datos sean procesados, este proceso puede tardar mucho tiempo.