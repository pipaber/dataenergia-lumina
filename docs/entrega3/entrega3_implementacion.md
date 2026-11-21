---
title: "DataEnergía Metropolitan · Lumina Energía"
subtitle: "Entrega 3: guía de implementación paso a paso, con evidencias"
author: "Yessenia Chirinos, Edgard Inga, Diego Sánchez y Piero Palacios"
date: 2026-11-21
lang: es
---
> Documento elaborado con asistencia de IA generativa (Claude). Los fragmentos de código se copian directamente de los scripts del repositorio y las salidas son resultados reales del proyecto en Google Cloud.

Esta guía es el entregable para quien tenga que implementar la plataforma: recorre, en orden, todo lo que se creó, con qué script y qué líneas lo hacen, y cómo comprobar que quedó funcionando. Cada paso tiene la misma estructura:

1. **Qué se crea** y para qué.
2. **El comando** que se ejecuta.
3. **El fragmento del script** que hace el trabajo.
4. **La evidencia:** la salida real y una captura de pantalla.

Los comandos se ejecutan desde la raíz del repositorio, en Bash (en Windows, Git Bash). El proyecto de Google Cloud se llama `tespipaber` y la región es `us-central1`.

## Mapa de la implementación

| Paso | Qué se crea | Script | Dónde corre |
|---|---|---|---|
| 0 | Configuración del proyecto | `infra/config.env` | Laptop |
| 1 | APIs y presupuesto con alertas | `infra/01_apis_presupuesto.sh` | Laptop |
| 2 | Data lake (2 buckets) y 5 datasets de BigQuery | `infra/02_buckets_bigquery.sh` | Laptop |
| 3 | Datos crudos de Kaggle en el lake | `infra/03_secreto_kaggle.sh`, `infra/04_ingesta_kaggle.sh` | VM temporal |
| 4 | Capa silver: lecturas limpias | `infra/05_spark_batch.sh silver` | Spark serverless |
| 5 | Capa gold: modelo de datos | `infra/05_spark_batch.sh gold` | Spark serverless |
| 6 | Tablas en BigQuery | `infra/06_cargar_bigquery.sh` | BigQuery |
| 7 | Streaming: Kafka y Spark Structured Streaming | `infra/07_vm_ops.sh` | VM `lumina-ops` |
| 8 | Orquestación del batch con Airflow | `src/dags/lumina_batch_diario.py` | VM `lumina-ops` |
| 9 | Vistas OLAP que unen historia y hoy | `infra/09_vistas_olap.sh` | BigQuery |
| 10 | Tarifas, pronóstico y mantenimiento | `infra/08`, `10`, `11` y `12` | BigQuery ML |
| 11 | API y dos aplicaciones web | `infra/13_api_cloud_run.sh` | Cloud Run |
| 12 | Apagado y control de costos | `infra/shutdown.sh`, `infra/teardown.sh` | Laptop |

## Paso 0 · Configuración

**Qué se crea.** Un archivo local con los datos del proyecto. Todos los scripts lo leen, así que ningún nombre queda escrito dentro del código.

```bash
cp infra/config.example.env infra/config.env    # completar PROJECT_ID, BILLING_ACCOUNT y KAGGLE_API_TOKEN
```

El archivo `config.env` no se sube al repositorio, porque contiene el token de Kaggle. Requisitos: [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) con sesión iniciada y una cuenta de Kaggle.

## Paso 1 · APIs y presupuesto

**Qué se crea.** Se habilitan los servicios de Google Cloud que usa el proyecto y un presupuesto de S/1,125 (≈ US$300) que avisa por correo al llegar a ≈ US$50, US$150 y US$300.

```bash
bash infra/01_apis_presupuesto.sh
```

**Cómo lo hace.** Primero habilita las APIs:

```bash
gcloud services enable --project "$PROJECT_ID" \
  billingbudgets.googleapis.com \
  cloudresourcemanager.googleapis.com \
  serviceusage.googleapis.com \
  iam.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  dataproc.googleapis.com \
  secretmanager.googleapis.com \
  pubsub.googleapis.com
```

*Fragmento de [`infra/01_apis_presupuesto.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/01_apis_presupuesto.sh), líneas 15 a 26.*

Luego crea el presupuesto con sus umbrales de alerta. El monto va en soles porque la cuenta de facturación está en esa moneda:

```bash
gcloud billing budgets create \
  --billing-account "$BILLING_ACCOUNT" \
  --display-name "$BUDGET_NAME" \
  --budget-amount "$BUDGET_AMOUNT" \
  --filter-projects "projects/$PROJECT_ID" \
  --threshold-rule percent=0.1667 \
  --threshold-rule percent=0.5 \
  --threshold-rule percent=1.0 \
  --threshold-rule percent=0.5,basis=forecasted-spend
```

*Fragmento de [`infra/01_apis_presupuesto.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/01_apis_presupuesto.sh), líneas 35 a 43.*

**Evidencia.**

![Presupuesto del proyecto con sus alertas, en Facturación](img/guia/01_presupuesto.png)

## Paso 2 · Data lake y datasets de BigQuery

**Qué se crea.** Dos buckets (contenedores de archivos) en Cloud Storage, que forman el data lake, y cinco datasets (grupos de tablas) en BigQuery:

| Recurso | Para qué |
|---|---|
| `gs://tespipaber-lumina-raw` | Datos tal como llegan: Kaggle y la copia de cada lectura del streaming |
| `gs://tespipaber-lumina-curated` | Capas silver y gold en Parquet, código de los trabajos y *checkpoints* |
| `lumina_curated` | Modelo de datos histórico (capa batch) |
| `lumina_rt` | Resultados del streaming (capa de velocidad) |
| `lumina_olap` | Vistas que unen historia y hoy |
| `lumina_ml` | Modelos y sus resultados |
| `lumina_muestra` | Corridas de prueba con pocos datos |

```bash
bash infra/02_buckets_bigquery.sh
```

**Cómo lo hace.** Crea cada bucket sin acceso público:

```bash
gcloud storage buckets create "gs://$bucket" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --default-storage-class STANDARD \
  --uniform-bucket-level-access \
  --public-access-prevention \
  --soft-delete-duration 0
```

*Fragmento de [`infra/02_buckets_bigquery.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/02_buckets_bigquery.sh), líneas 14 a 20.*

Y crea los datasets, solo si no existen:

```bash
for ds in $BQ_DATASETS; do
  if bq --project_id "$PROJECT_ID" show --dataset "$PROJECT_ID:$ds" >/dev/null 2>&1; then
    echo "Ya existe $ds"
  else
    bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" mk --dataset \
      --label proyecto:lumina --label curso:bigdata-utec \
      --description "Lumina Energía · capa ${ds#lumina_}" "$PROJECT_ID:$ds"
  fi
done
```

*Fragmento de [`infra/02_buckets_bigquery.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/02_buckets_bigquery.sh), líneas 31 a 39.*

**Evidencia.** Consulta a BigQuery con los datasets creados:

```text
+----------------+-----------+
| dataset        | ubicacion |
+----------------+-----------+
| lumina_curated | US        |
| lumina_ml      | US        |
| lumina_muestra | US        |
| lumina_olap    | US        |
| lumina_rt      | US        |
+----------------+-----------+
```

![Los dos buckets del data lake en Cloud Storage](img/guia/02_buckets.png)

![Los cinco datasets en el explorador de BigQuery](img/guia/02_datasets.png)

## Paso 3 · Ingesta de los datos de Kaggle

**Qué se crea.** Los 9.63 GiB de datos crudos en la capa raw del lake: 112 bloques de lecturas cada 30 minutos, los maestros de hogares, el clima y la demanda nacional.

```bash
bash infra/03_secreto_kaggle.sh     # guarda el token de Kaggle en Secret Manager
bash infra/04_ingesta_kaggle.sh     # crea la VM, espera la descarga y la borra
```

**Cómo lo hace.** La descarga corre dentro de Google Cloud, en una VM temporal. Así no usa el internet de la laptop, y la VM se borra sola a las 3 horas aunque algo falle:

```bash
gcloud compute instances create "$INGEST_VM" \
  --project "$PROJECT_ID" --zone "$ZONE" \
  --machine-type e2-standard-2 \
  --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 100GB --boot-disk-type pd-standard \
  --service-account "$SA_EMAIL" --scopes cloud-platform \
  --max-run-duration 3h --instance-termination-action DELETE \
  --labels "$LABELS" \
  --metadata "project-id=$PROJECT_ID,bucket-raw=$BUCKET_RAW,kaggle-secret=$KAGGLE_SECRET" \
  --metadata-from-file "startup-script=$REPO_DIR/src/ingestion/kaggle_to_gcs.sh"
```

*Fragmento de [`infra/04_ingesta_kaggle.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/04_ingesta_kaggle.sh), líneas 33 a 42.*

Al arrancar, la VM ejecuta este script, que lee el token desde Secret Manager, descarga cada dataset y lo copia al lake:

```bash
download() {  # $1 = dataset de Kaggle, $2 = carpeta destino en raw
  local dir="/data/$2"
  mkdir -p "$dir"
  /opt/kaggle/bin/kaggle datasets download -d "$1" -p "$dir" --unzip
  # Algunos datasets traen zips anidados (p. ej. halfhourly_dataset.zip)
  find "$dir" -name '*.zip' -print0 | while IFS= read -r -d '' z; do
    unzip -q -o "$z" -d "$(dirname "$z")" && rm -f "$z"
  done
  du -sh "$dir"
  gcloud storage cp -r "$dir"/* "gs://$BUCKET_RAW/kaggle/$2/" --quiet
}

download jeanmidev/smart-meters-in-london            smart_meters_london
download albertovidalrod/electricity-consumption-uk-20092022 uk_national_demand
```

*Fragmento de [`src/ingestion/kaggle_to_gcs.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/ingestion/kaggle_to_gcs.sh), líneas 48 a 61.*

**Evidencia.** Contenido de la capa raw después de la ingesta:

```text
$ gcloud storage ls gs://tespipaber-lumina-raw/kaggle/smart_meters_london/
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/daily_dataset/
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/halfhourly_dataset/
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/hhblock_dataset/
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/acorn_details.csv  (0.1 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/daily_dataset.csv  (351.8 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/darksky_parameters_documentation.html  (0.1 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/informations_households.csv  (0.2 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/uk_bank_holidays.csv  (0.0 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/weather_daily_darksky.csv  (0.3 MB)
gs://tespipaber-lumina-raw/kaggle/smart_meters_london/weather_hourly_darksky.csv  (2.0 MB)
```

![Carpeta kaggle/ del bucket raw en Cloud Storage](img/guia/03_raw.png)

## Paso 4 · Capa silver: limpieza con Spark

**Qué se crea.** Las lecturas limpias y tipadas en Parquet. Spark corre en modo *serverless*: Google asigna las máquinas mientras dura el trabajo y no queda nada encendido.

```bash
bash infra/05_spark_batch.sh silver --muestra    # prueba barata con 2 bloques
bash infra/05_spark_batch.sh silver              # los 112 bloques: 10.6 min, ≈ US$0.21
```

**Cómo lo hace.** El script sube el código al lake y envía el trabajo a Spark, con un tope de ejecutores para controlar el costo:

```bash
gcloud dataproc batches submit pyspark "$CODIGO/$JOB.py" \
  --project "$PROJECT_ID" --region "$REGION" \
  --batch "$BATCH_ID" \
  --version "$RUNTIME" \
  --service-account "$SA_EMAIL" \
  --subnet default \
  --py-files "$CODIGO/lumina_common.py" \
  --ttl 2h \
  --labels "$LABELS,job=$JOB" \
  --properties "spark.executor.instances=2,spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS,spark.sql.session.timeZone=UTC,spark.sql.shuffle.partitions=128" \
  -- --entrada "$ENTRADA" --salida "$SALIDA" "${EXTRA[@]}" || estado=$?
```

*Fragmento de [`infra/05_spark_batch.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/05_spark_batch.sh), líneas 57 a 67.*

La limpieza está en `silver.py`. Convierte el texto a número, marca las lecturas vacías y las que no caen en una media hora exacta, y elimina duplicados:

```python
kwh_txt = F.trim("kwh_txt")
ts = F.to_timestamp(F.substring("tstp", 1, 19), "yyyy-MM-dd HH:mm:ss")
df = (
    df.withColumn("ts_utc", ts)
    .withColumn("kwh", F.when(kwh_txt.rlike(NUMERO), kwh_txt.cast("double")))
    .withColumn("es_nulo", F.col("kwh").isNull())
    .withColumn("fuera_de_grilla",
                ~(F.minute("ts_utc").isin(0, 30) & (F.second("ts_utc") == 0)))
    .withColumn("bloque", F.regexp_extract(F.input_file_name(), r"block_(\d+)", 1).cast("int"))
    .where(F.col("lclid").isNotNull() & F.col("ts_utc").isNotNull())
    .dropDuplicates(["lclid", "ts_utc"])
    .withColumn("fecha", F.to_date("ts_utc"))
    .withColumn("anio_mes", F.date_format("ts_utc", "yyyy-MM"))
    .select("lclid", "ts_utc", "fecha", "kwh", "es_nulo", "fuera_de_grilla", "bloque", "anio_mes")
)
```

*Fragmento de [`src/batch/silver.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/batch/silver.py), líneas 46 a 60.*

**Evidencia.** Trabajos de Spark del proyecto, con su estado. Los que empiezan con `lumina-af-` los lanzó Airflow:

```text
BATCH_ID                            ESTADO     CREADO (UTC)
lumina-silver-20261020-162927       SUCCEEDED  2026-10-20 16:29:44
lumina-gold-20261020-163354         SUCCEEDED  2026-10-20 16:34:12
lumina-silver-20261020-164754       SUCCEEDED  2026-10-20 16:48:10
lumina-gold-20261020-165907         SUCCEEDED  2026-10-20 16:59:25
lumina-af-silver-20261020t195405    FAILED     2026-10-20 19:57:39
lumina-af-silver-20261020t195801    FAILED     2026-10-20 19:58:08
lumina-af-silver-20261103t203318    SUCCEEDED  2026-11-03 20:33:26
lumina-af-gold-20261103t203318      FAILED     2026-11-03 20:36:22
lumina-af-gold-20261103t203318-3    SUCCEEDED  2026-11-03 20:41:32
lumina-af-silver-20261103t205848-1  SUCCEEDED  2026-11-03 20:58:55
lumina-af-gold-20261103t205848-1    FAILED     2026-11-03 21:02:20
lumina-af-gold-20261103t205848-2    SUCCEEDED  2026-11-03 21:05:37
```

![Lista de trabajos (batches) de Spark serverless](img/guia/04_spark_batches.png)

## Paso 5 · Capa gold: modelo de datos

**Qué se crea.** El modelo estrella en Parquet: dimensiones (hogar, fecha, segmento), hechos de consumo y de carga, el perfil de cada hogar y el diagnóstico de calidad de cada medidor.

```bash
bash infra/05_spark_batch.sh gold                # 21.8 min, ≈ US$0.44
```

**Cómo lo hace.** Por ejemplo, la carga de cada transformador se obtiene sumando el consumo de sus hogares en cada media hora:

```python
def fact_carga_transformador(consumo: DataFrame, hogares: DataFrame) -> DataFrame:
    return (
        consumo.join(F.broadcast(hogares.select("lclid", "zona", "transformador")), "lclid")
        .groupBy("zona", "transformador", "ts_utc", "fecha")
        .agg(F.round(F.sum("kwh"), 4).alias("kwh_total"),
             F.count("kwh").alias("n_medidores"))
        .withColumn("kw_medio", F.round(F.col("kwh_total") * 2, 4))   # kWh en 30 min -> kW
    )
```

*Fragmento de [`src/batch/gold.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/batch/gold.py), líneas 74 a 81.*

Y cada medidor recibe un diagnóstico según su historia de lecturas:

```python
return df.withColumn(
    "diagnostico",
    F.when(F.col("n_validas") == 0, "sin_datos")
    .when(F.col("max_racha_ceros") >= 48, "ceros_prolongados")
    .when(F.col("cobertura_pct") < 90, "cobertura_baja")
    .when(F.col("n_huecos") > 10, "huecos_frecuentes")
    .otherwise("ok"),
)
```

*Fragmento de [`src/batch/gold.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/batch/gold.py), líneas 142 a 149.*

**Evidencia.** Tablas de la capa gold en el lake:

```text
$ gcloud storage ls gs://tespipaber-lumina-curated/gold/
gs://tespipaber-lumina-curated/gold/acorn_atributos/
gs://tespipaber-lumina-curated/gold/dim_acorn/
gs://tespipaber-lumina-curated/gold/dim_fecha/
gs://tespipaber-lumina-curated/gold/dim_hogar/
gs://tespipaber-lumina-curated/gold/dq_medidor/
gs://tespipaber-lumina-curated/gold/fact_carga_transformador_30min/
gs://tespipaber-lumina-curated/gold/fact_carga_zona_30min/
gs://tespipaber-lumina-curated/gold/fact_clima_dia/
gs://tespipaber-lumina-curated/gold/fact_clima_hora/
gs://tespipaber-lumina-curated/gold/fact_consumo_30min/
gs://tespipaber-lumina-curated/gold/fact_consumo_diario/
gs://tespipaber-lumina-curated/gold/fact_demanda_nacional_30min/
gs://tespipaber-lumina-curated/gold/perfil_hogar/
```

## Paso 6 · Tablas en BigQuery

**Qué se crea.** Las 13 tablas gold cargadas en `lumina_curated`, más tres tablas derivadas. Cargar Parquet a BigQuery no tiene costo.

```bash
bash infra/06_cargar_bigquery.sh
```

**Cómo lo hace.** Una función carga cada tabla. Las tres tablas grandes se particionan por fecha y se agrupan por medidor, para que una consulta lea solo lo que necesita:

```bash
cargar() {  # $1 tabla, resto: opciones de particionado/clustering
  local tabla="$1"; shift
  "${BQ[@]}" load --replace --source_format PARQUET "$@" \
    "$DS.$tabla" "$GOLD/$tabla/*.parquet" >/dev/null
  echo "  $DS.$tabla: $("${BQ[@]}" query --nouse_legacy_sql --format csv \
    "SELECT COUNT(*) FROM \`$PROJECT_ID.$DS.$tabla\`" | tail -1) filas"
}

log "Cargando dimensiones y hechos desde $GOLD"
for t in dim_hogar dim_fecha dim_acorn acorn_atributos fact_clima_hora fact_clima_dia \
         fact_demanda_nacional_30min fact_carga_zona_30min perfil_hogar dq_medidor; do
  cargar "$t"
done
cargar fact_consumo_30min \
  --time_partitioning_field fecha --time_partitioning_type DAY --clustering_fields lclid
cargar fact_consumo_diario \
  --time_partitioning_field fecha --time_partitioning_type MONTH --clustering_fields lclid
cargar fact_carga_transformador_30min \
  --time_partitioning_field fecha --time_partitioning_type MONTH --clustering_fields zona,transformador
```

*Fragmento de [`infra/06_cargar_bigquery.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/06_cargar_bigquery.sh), líneas 20 a 38.*

Después se crean las tablas derivadas con SQL. Esta guarda una fila por medidor y día, con sus 48 lecturas dentro (`STRUCT` y `ARRAY`):

```sql
CREATE OR REPLACE TABLE `${P}.${DS}.consumo_dia_anidado`
PARTITION BY DATE_TRUNC(fecha, MONTH)
CLUSTER BY lclid AS
SELECT
  c.lclid,
  c.fecha,
  STRUCT(h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador) AS hogar,
  ROUND(SUM(c.kwh), 4) AS kwh_total,
  ROUND(SUM(IF(c.es_punta, c.kwh, 0)), 4) AS kwh_punta,
  COUNT(c.kwh) AS n_lecturas,
  ARRAY_AGG(STRUCT(c.periodo_local, c.ts_utc, c.kwh, c.es_nulo, c.es_punta) ORDER BY c.ts_utc) AS lecturas
FROM `${P}.${DS}.fact_consumo_30min` c
JOIN `${P}.${DS}.dim_hogar` h USING (lclid)
WHERE c.fecha IS NOT NULL
GROUP BY c.lclid, c.fecha, h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador;
```

*Fragmento de [`src/sql/gold_derivadas.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/gold_derivadas.sql), líneas 17 a 31.*

**Evidencia.** Tablas de `lumina_curated` con sus filas y tamaño:

```text
+--------------------------------+-----------+------+
| tabla                          | filas     | gib  |
+--------------------------------+-----------+------+
| fact_consumo_30min             | 167811461 | 8.28 |
| fact_carga_transformador_30min | 4394004   | 0.23 |
| consumo_dia_anidado            | 3510403   | 4.34 |
| fact_consumo_diario            | 3510403   | 0.25 |
| perfil_hogar                   | 533804    | 0.04 |
| fact_carga_zona_30min          | 317746    | 0.01 |
| fact_demanda_nacional_30min    | 279264    | 0.02 |
| fact_clima_hora                | 21165     | 0.0  |
| dim_tarifa_dtou                | 17520     | 0.0  |
| acorn_atributos                | 14042     | 0.0  |
| dim_hogar                      | 5566      | 0.0  |
| dq_medidor                     | 5561      | 0.0  |
| umbral_zona                    | 3072      | 0.0  |
| clima_noaa_heathrow            | 1461      | 0.0  |
| dim_fecha                      | 882       | 0.0  |
| fact_clima_dia                 | 879       | 0.0  |
| dim_acorn                      | 18        | 0.0  |
+--------------------------------+-----------+------+
```

Particiones de las dos tablas de consumo:

```text
+---------------------+-------------+----------+----------+
| tabla               | particiones | primera  | ultima   |
+---------------------+-------------+----------+----------+
| fact_consumo_30min  | 829         | 20111123 | 20140228 |
| fact_consumo_diario | 28          | 201111   | 201402   |
+---------------------+-------------+----------+----------+
```

Validación contra la fuente: los pares medidor-día y la energía total coinciden exactamente con el agregado diario que publica Kaggle.

```text
+-------------+-------+
| medidor_dia | gwh   |
+-------------+-------+
| 3510403     | 35.54 |
+-------------+-------+
```

Dos filas de la tabla anidada: cada una trae el hogar como `STRUCT` y sus 48 lecturas como `ARRAY`.

```text
+-----------+------------+--------+------+------------+---------+
| lclid     | fecha      | tarifa | zona | n_lecturas | kwh_18h |
+-----------+------------+--------+------+------------+---------+
| MAC004400 | 2013-01-16 | ToU    | 6    | 48         | 3.687   |
| MAC004436 | 2013-01-16 | ToU    | 3    | 48         | 0.134   |
+-----------+------------+--------+------+------------+---------+
```

![Tablas de lumina_curated en BigQuery](img/guia/06_bigquery_tablas.png)

![Detalles de fact_consumo_30min: particionada por día y agrupada por medidor](img/guia/06_bigquery_particion.png)

## Paso 7 · Streaming: Kafka y Spark Structured Streaming

**Qué se crea.** Una VM con Kafka, Spark Structured Streaming y Airflow en contenedores Docker. Se enciende solo para probar o demostrar, y se destruye al terminar.

```bash
bash infra/07_vm_ops.sh tablas                     # crea las tablas de salida en lumina_rt
bash infra/07_vm_ops.sh crear streaming,airflow    # crea y enciende la VM (~8 min)
bash infra/07_vm_ops.sh replay --desde 2013-01-14 --dias 2 --segundos-intervalo 1 \
  --concentrador-caido Z3-T07 --caida-desde "'2013-01-14 06:00:00'" --anomalias 20
bash infra/07_vm_ops.sh destruir                   # borra la VM: costo 0 entre demos
```

**Cómo lo hace.** El script crea la VM. Al arrancar, la VM instala Docker, descarga el código desde el lake y levanta los servicios:

```bash
gcloud compute instances create "$OPS_VM" "${VM[@]}" \
  --machine-type e2-standard-4 \
  --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 30GB --boot-disk-type pd-balanced \
  --service-account "$SA_EMAIL" --scopes cloud-platform \
  --resource-policies "$HORARIO" \
  --labels "$LABELS" \
  --metadata "^;^project-id=$PROJECT_ID;bucket-curated=$BUCKET_CURATED;bucket-raw=$BUCKET_RAW;region=$REGION;sa-spark=$SA_SPARK;perfiles=$PERFILES" \
  --metadata-from-file "startup-script=$REPO_DIR/infra/ops/vm_startup.sh"
```

*Fragmento de [`infra/07_vm_ops.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/07_vm_ops.sh), líneas 119 a 127.*

El programa de streaming lee las lecturas desde Kafka:

```python
def kafka(self, topico: str) -> DataFrame:
    return (self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.args.bootstrap)
            .option("subscribe", topico)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 200_000)
            .load())
```

*Fragmento de [`src/streaming/streaming_job.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/streaming/streaming_job.py), líneas 122 a 128.*

Compara cada lectura con el perfil de su propio hogar. Hay alerta si supera el mayor de tres valores: 4 veces su percentil 95, su media más 8 desviaciones, o 1.5 kWh:

```python
def consulta_anomalias(ctx: Contexto, lect: DataFrame):
    umbral = F.greatest(F.col("kwh_p95") * FACTOR_P95, F.col("kwh_media") + SIGMAS * F.col("kwh_std"),
                        F.lit(KWH_MINIMO))
    base = (lect.withColumn("fecha", F.to_date("ts_utc"))
            .withColumn("periodo_local", periodo_local("ts_utc")))

    def lote(df: DataFrame, batch_id: int) -> None:
        ctx.refrescar()
        if df.isEmpty():
            return
        alertas = columnas_alerta(
            df.join(ctx.fechas, "fecha", "left")
              .join(ctx.hogar, "lclid", "left")
              .join(ctx.perfil, ["lclid", "tipo_dia", "periodo_local"], "left")
              .withColumn("umbral", umbral)
              .where((F.col("kwh") < 0) | (F.col("kwh") > F.col("umbral"))),
            F.when(F.col("kwh") < 0, "lectura_negativa").otherwise("consumo_anomalo"), "kwh", "umbral").cache()
        n = alertas.count()
        if n:
            ctx.a_bigquery(alertas, "alertas")
```

*Fragmento de [`src/streaming/streaming_job.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/streaming/streaming_job.py), líneas 162 a 181.*

Y guarda una copia cruda de cada lectura en el lake, para que el batch de la noche la vuelva historia:

```python
def consulta_bronze(ctx: Contexto):
    """Copia cruda de cada lectura tal como llegó a Kafka; el file sink de Spark es exactly-once."""
    crudo = (ctx.kafka("meter.readings")
             .select(F.col("value").cast("string").alias("valor_json"),
                     F.col("topic").alias("topico"), F.col("partition").alias("particion"),
                     F.col("offset"), F.col("timestamp").alias("ts_kafka"))
             .withColumn("v", F.from_json("valor_json", LECTURA))
             .select(F.col("v.lclid").alias("lclid"), F.col("v.ts_utc").alias("ts_utc"),
                     F.col("v.kwh").alias("kwh"), "valor_json", "topico", "particion", "offset", "ts_kafka")
             .withColumn("fecha", F.to_date(F.substring("ts_utc", 1, 10))))
    return (crudo.writeStream.format("parquet").queryName("bronze")
            .option("path", ctx.args.bronze)
            .option("checkpointLocation", f"{ctx.args.checkpoints}/bronze")
            .partitionBy("fecha")
            .trigger(processingTime="1 minute")     # archivos más grandes y menos numerosos
            .start())
```

*Fragmento de [`src/streaming/streaming_job.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/streaming/streaming_job.py), líneas 295 a 310.*

**Evidencia.** Prueba en vivo del 3 de noviembre de 2026: se reprodujeron 523,868 lecturas de 5,508 hogares, con un concentrador caído y 20 consumos anómalos. Alertas que generó:

```text
+--------------------+---------+-----------+
| tipo               | alertas | medidores |
+--------------------+---------+-----------+
| consumo_anomalo    | 109     | 78        |
| medidor_silencioso | 57      | 57        |
| pico_zona          | 33      | 0         |
+--------------------+---------+-----------+
```

Los 52 medidores del concentrador caído (Z3-T07) están entre los 57 medidores en silencio. Tablas de salida del streaming:

```text
+------------------+-------+
| tabla            | filas |
+------------------+-------+
| alertas          | 2138  |
| carga_zona_30min | 3408  |
| demanda_red      | 432   |
+------------------+-------+
```

Copia de las lecturas en el lake, particionada por fecha:

```text
$ gcloud storage ls gs://tespipaber-lumina-raw/streaming/lecturas/
gs://tespipaber-lumina-raw/streaming/lecturas/_spark_metadata/
gs://tespipaber-lumina-raw/streaming/lecturas/fecha=2013-01-14/
gs://tespipaber-lumina-raw/streaming/lecturas/fecha=2013-01-15/
```

![Alertas del streaming en la tabla lumina_rt.alertas](img/guia/07_alertas.png)

![Interfaz web de Spark en la VM: las cinco consultas de Structured Streaming activas mientras llegan las lecturas](img/guia/07_streaming_logs.png)

## Paso 8 · Orquestación con Airflow

**Qué se crea.** Un flujo (DAG) que encadena todo el batch: espera los datos, lanza silver y gold en Spark, carga 13 tablas en BigQuery, recalcula las derivadas y corre 3 controles de calidad.

```bash
bash infra/07_vm_ops.sh dag muestra    # dispara el DAG con 2 bloques
bash infra/07_vm_ops.sh tunel          # Airflow en http://localhost:8080
```

**Cómo lo hace.** Las dos tareas de Spark usan los mismos `silver.py` y `gold.py` del paso 4. Cada intento lleva su número en el nombre, para que un reintento no choque con el anterior:

```python
silver = DataprocCreateBatchOperator(
    task_id="spark_silver", project_id=PROYECTO, region=REGION,
    batch_id="lumina-af-silver-{{ ts_nodash | lower }}-{{ ti.try_number }}",   # un id por intento
    batch=batch_spark("silver", f"gs://{BUCKET_RAW}/kaggle", f"{PREFIJO}/silver",
                      ["--entrada-streaming", BRONZE_STREAMING]))

gold = DataprocCreateBatchOperator(
    task_id="spark_gold", project_id=PROYECTO, region=REGION,
    batch_id="lumina-af-gold-{{ ts_nodash | lower }}-{{ ti.try_number }}",
    batch=batch_spark("gold", f"{PREFIJO}/silver", f"{PREFIJO}/gold"))
```

*Fragmento de [`src/dags/lumina_batch_diario.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/dags/lumina_batch_diario.py), líneas 121 a 130.*

El orden de las tareas se declara en una línea:

```python
inicio >> verificar_raw >> silver >> gold >> cargas >> derivadas >> calidad >> fin
```

*Fragmento de [`src/dags/lumina_batch_diario.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/dags/lumina_batch_diario.py), líneas 169 a 169.*

**Evidencia.** En la corrida del 3 de noviembre de 2026 las 22 tareas terminaron bien. El primer intento de gold falló por cuota de CPU y el reintento automático lo resolvió; se ve en la lista de trabajos del paso 4 (`lumina-af-gold-…-1` falló y `…-2` terminó bien).

![Grafo del DAG lumina_batch_diario en Airflow, con sus tareas en verde](img/guia/08_airflow_dag.png)

## Paso 9 · Vistas OLAP: historia y hoy en una sola consulta

**Qué se crea.** Cinco vistas en `lumina_olap`. Una vista es una consulta guardada que se usa como una tabla; no ocupa espacio ni tiene costo hasta que se consulta.

```bash
bash infra/09_vistas_olap.sh
```

**Cómo lo hace.** La vista principal une la carga histórica (batch) con la de hoy (streaming). Del streaming solo toma lo posterior al último dato del batch: si hay diferencia, manda el batch.

```sql
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_carga_zona_30min` AS
WITH batch AS (
  SELECT ts_utc, fecha, zona, kwh_total, n_medidores, kw_medio / n_medidores AS kw_por_medidor,
         'batch' AS fuente
  FROM `${P}.lumina_curated.fact_carga_zona_30min`
),
corte AS (SELECT MAX(ts_utc) AS ultimo_batch FROM batch),
rt AS (
  SELECT ventana_inicio AS ts_utc, fecha, zona, kwh_total, n_lecturas AS n_medidores, kw_por_medidor,
         'streaming' AS fuente
  FROM `${P}.lumina_rt.carga_zona_30min`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY zona, ventana_inicio ORDER BY ts_procesado DESC) = 1
),
unida AS (
  SELECT * FROM batch
  UNION ALL
  SELECT rt.* FROM rt CROSS JOIN corte WHERE rt.ts_utc > corte.ultimo_batch
)
SELECT
  u.ts_utc, u.fecha, u.zona, u.kwh_total, u.n_medidores, ROUND(u.kw_por_medidor, 4) AS kw_por_medidor,
  u.fuente,
  EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) AS hora_local,
  f.tipo_dia, f.estacion, t.kw_p95,
  COALESCE(u.kw_por_medidor > t.kw_p95
           AND EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) BETWEEN 17 AND 20, FALSE) AS es_pico
FROM unida u
LEFT JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
LEFT JOIN `${P}.lumina_curated.umbral_zona` t
  ON t.zona = u.zona AND t.tipo_dia = f.tipo_dia AND t.estacion = f.estacion
 AND t.periodo_local = EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) * 2
                       + DIV(EXTRACT(MINUTE FROM DATETIME(u.ts_utc, 'Europe/London')), 30) + 1;
```

*Fragmento de [`src/sql/olap_vistas.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/olap_vistas.sql), líneas 8 a 39.*

**Evidencia.**

```text
+---------------------------+
| vista                     |
+---------------------------+
| v_alertas                 |
| v_calidad_medidores       |
| v_carga_zona_30min        |
| v_consumo_segmento_diario |
| v_red_vs_muestra          |
+---------------------------+
```

## Paso 10 · Tarifas, pronóstico y mantenimiento

**Qué se crea.** El análisis de la tarifa dinámica y tres modelos de BigQuery ML, entrenados con SQL sobre los mismos datos.

```bash
bash infra/08_tarifas_dtou.sh     # calendario oficial de precios 2013
bash infra/11_tarifas.sh          # factura con cada tarifa y respuesta a los eventos de precio
bash infra/10_pronostico.sh       # pronóstico del día siguiente, por zona y por hogar
bash infra/12_mantenimiento.sh    # riesgo de falla por medidor y transformadores sobrecargados
```

**Cómo lo hace.** El modelo de pronóstico por zona se entrena con una sola sentencia SQL:

```sql
CREATE OR REPLACE MODEL `${P}.lumina_ml.modelo_zona_d1`
OPTIONS (model_type = 'BOOSTED_TREE_REGRESSOR', input_label_cols = ['y'],
         max_iterations = 60, early_stop = TRUE, subsample = 0.8,
         data_split_method = 'RANDOM', data_split_eval_fraction = 0.1) AS
SELECT y, zona_txt, periodo_local, dia_semana, tipo_dia, estacion, es_feriado, mes,
       lag_1d, lag_7d, media_7d, temperatura_c, humedad, temp_max_c, temp_min_c
FROM `${P}.lumina_ml.features_zona_30min`
WHERE conjunto = 'entrenamiento';
```

*Fragmento de [`src/sql/pronostico.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/pronostico.sql), líneas 43 a 50.*

El de fallas de medidores es un clasificador: predice si un medidor fallará el mes siguiente.

```sql
CREATE OR REPLACE MODEL `${P}.lumina_ml.modelo_falla_medidor`
OPTIONS (model_type = 'BOOSTED_TREE_CLASSIFIER', input_label_cols = ['falla_mes_siguiente'],
         auto_class_weights = TRUE, max_iterations = 50, early_stop = TRUE,
         data_split_method = 'RANDOM', data_split_eval_fraction = 0.1) AS
SELECT falla_mes_siguiente, falla, dias_sin_dato, dias_baja, dias_cero, kwh_medio, fallas_3m,
       dias_malos_3m, dias_cero_3m, ratio_consumo, meses_activo, mes_anio, acorn_grupo, tarifa
FROM `${P}.lumina_ml.features_medidor_mes`
WHERE conjunto = 'entrenamiento';
```

*Fragmento de [`src/sql/mantenimiento.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/mantenimiento.sql), líneas 64 a 71.*

**Evidencia.** Tablas y resultados en `lumina_ml`:

```text
+--------------------------+---------+
| tabla                    | filas   |
+--------------------------+---------+
| factura_tarifas_2013     | 5107    |
| features_hogar_diario    | 3439325 |
| features_medidor_mes     | 107840  |
| features_zona_30min      | 278711  |
| metricas_mantenimiento   | 4       |
| metricas_pronostico      | 4       |
| modelo_falla_medidor     | 17      |
| modelo_hogar_d1          | 21      |
| modelo_zona_d1           | 20      |
| prediccion_falla_medidor | 36537   |
| pronostico_hogar_d1      | 290034  |
| pronostico_zona_d1       | 22280   |
| ranking_medidores        | 5108    |
| ranking_transformadores  | 112     |
| respuesta_eventos_2013   | 4       |
+--------------------------+---------+
```

Error del pronóstico en meses que el modelo no vio (enero y febrero de 2014), frente a suponer que mañana será igual que hoy:

```text
+-------------------+--------------------------+--------+----------+----------+
| modelo            | metodo                   | n      | mape_pct | wape_pct |
+-------------------+--------------------------+--------+----------+----------+
| hogar · día       | boosted tree             | 290034 | 39.75    | 18.53    |
| hogar · día       | ingenuo (igual que ayer) | 290034 | 28.32    | 21.26    |
| zona · media hora | boosted tree             | 22280  | 4.4      | 4.33     |
| zona · media hora | ingenuo (igual que ayer) | 22280  | 6.91     | 6.8      |
+-------------------+--------------------------+--------+----------+----------+
```

Mantenimiento: fallas encontradas según cuántos medidores se inspeccionan cada mes (julio a diciembre de 2013):

```text
+-------------------------------------+----------------+--------------------+---------------+---------------+
| metodo                              | inspeccionados | fallas_encontradas | precision_pct | cobertura_pct |
+-------------------------------------+----------------+--------------------+---------------+---------------+
| ingenuo: los que fallaron este mes  | 749            | 554                | 74.0          | 53.9          |
| modelo: mismo número que el ingenuo | 749            | 551                | 73.6          | 53.7          |
| modelo: 5 % con mayor riesgo        | 1585           | 638                | 40.3          | 62.1          |
| al azar (tasa de falla)             | 31429          | 1027               | 3.3           | 100.0         |
+-------------------------------------+----------------+--------------------+---------------+---------------+
```

Factura media de 2013 con cada tarifa:

```text
+--------+---------+------------------+---------------+------------+
| tarifa | hogares | factura_dinamica | factura_plana | ahorro_pct |
+--------+---------+------------------+---------------+------------+
| Std    | 4069    | 524.54           | 532.42        | 1.48       |
| ToU    | 1038    | 475.85           | 489.27        | 2.74       |
+--------+---------+------------------+---------------+------------+
```

![Evaluación del modelo modelo_zona_d1 en BigQuery ML](img/guia/10_bqml_modelo.png)

## Paso 11 · API y aplicaciones web

**Qué se crea.** Un servicio en Cloud Run que expone los datos como API y sirve dos aplicaciones web: la app del cliente y la consola interna de Lumina.

```bash
bash infra/13_api_cloud_run.sh desplegar       # construye la imagen y publica el servicio
bash infra/13_api_cloud_run.sh desde-github    # igual, tomando el código de GitHub
```

**Cómo lo hace.** Cada ruta de la API es una consulta parametrizada a BigQuery, con un tope de datos por consulta. Esta devuelve la carga de una zona en un día:

```python
@app.get("/zonas/{zona}/carga")
def carga_zona(zona: int, fecha: date = Query(..., examples=["2013-01-16"])):
    """Carga de la zona cada 30 min. `fuente` dice si el dato es definitivo (batch) o provisional (streaming)."""
    return uno_o_404(consultar(f"""
        SELECT ts_utc, hora_local, kw_por_medidor, kw_p95, es_pico, n_medidores, fuente
        FROM {OLAP}.v_carga_zona_30min`
        WHERE zona = @zona AND fecha = @fecha ORDER BY ts_utc""", zona=zona, fecha=fecha), f"la zona {zona} el {fecha}")
```

*Fragmento de [`src/api/main.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/api/main.py), líneas 135 a 141.*

El despliegue usa una cuenta de servicio que solo puede leer, y limita el servicio a una instancia:

```bash
gcloud run deploy "$SERVICIO" --project "$PROJECT_ID" --region "$REGION" \
  --source "$REPO_DIR/src/api" --quiet \
  --service-account "$SA_EMAIL" \
  --allow-unauthenticated \
  --min-instances 0 --max-instances 1 --concurrency 20 \
  --cpu 1 --memory 512Mi --timeout 60 \
  --set-env-vars "PROYECTO=$PROJECT_ID" \
  --labels "$LABELS"
```

*Fragmento de [`infra/13_api_cloud_run.sh`](https://github.com/pipaber/dataenergia-lumina/blob/main/infra/13_api_cloud_run.sh), líneas 46 a 53.*

**Evidencia.** Configuración del servicio en línea:

```text
servicio:            lumina-api
URL:                 https://lumina-api-csascndwia-uc.a.run.app
revisión en línea:   lumina-api-00004-q6b
instancias:          mínimo 1, máximo 1
recursos:            {'cpu': '1', 'memory': '512Mi'}
cuenta de servicio:  lumina-api@tespipaber.iam.gserviceaccount.com
acceso:              INGRESS_TRAFFIC_ALL
```

| Aplicación | Dirección |
|---|---|
| App del cliente | <https://lumina-api-csascndwia-uc.a.run.app/app> |
| Consola interna | <https://lumina-api-csascndwia-uc.a.run.app/consola> |
| API (documentación) | <https://lumina-api-csascndwia-uc.a.run.app/docs> |

![Servicio lumina-api en Cloud Run](img/guia/11_cloud_run.png)

![Documentación interactiva de la API](img/guia/11_api_docs.png)

![App del cliente: precios de mañana y cuánto se ahorra](img/guia/11_app_cliente.png)

![Consola interna: vista del centro de control](img/guia/11_consola.png)

## Paso 12 · Apagado y control de costos

**Qué se hace.** Al final de cada sesión se apaga lo que cobra por hora. Al cerrar el curso se borra todo.

```bash
bash infra/07_vm_ops.sh destruir               # borra la VM de streaming
bash infra/13_api_cloud_run.sh apagar-demo     # la API vuelve a escalar a cero
bash infra/shutdown.sh                         # verifica que no quede nada encendido
bash infra/teardown.sh                         # fin del curso: borra todo (pide confirmación)
```

**Evidencia.** Cada corrida se anota en la [bitácora de costos](https://github.com/pipaber/dataenergia-lumina/blob/main/docs/bitacora_costos.log). El gasto acumulado del proyecto es de ≈ US$2.3 de los US$300 disponibles.

| Concepto | Costo medido |
|---|---|
| Ingesta de Kaggle (VM temporal) | ≈ US$0.01 |
| Batch completo, silver y gold (167.8 M de lecturas) | ≤ US$0.65 |
| Modelos de BigQuery ML (pronóstico y mantenimiento) | ≤ US$0.61 |
| Sesión en vivo de 70 minutos (VM y Spark) | ≤ US$0.43 |
| API y aplicaciones en Cloud Run | ≈ US$0 |
