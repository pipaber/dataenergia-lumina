"""DAG lumina_batch_diario: bronze -> silver -> gold (Spark serverless) -> BigQuery -> calidad.

Orquesta los mismos jobs y SQL que los scripts de infra/ (05_spark_batch.sh, 06_cargar_bigquery.sh):
  verificar_raw -> spark_silver -> spark_gold -> cargas BigQuery -> tablas derivadas -> controles de calidad
Silver consolida también raw/streaming (lo que guardó la capa de velocidad: sincronización 1).

Parámetro `muestra` (al disparar): procesa 2 bloques de medidores, escribe en
gs://<curated>/muestra y carga en el dataset lumina_muestra (~US$0.16). Sin él, todo el histórico
(~US$0.65 en Spark).

En producción los jobs serían incrementales por partición diaria; en el taller el dataset es
histórico y fijo, así que cada corrida reprocesa todo (idempotente: sobrescribe).
Variables de entorno del contenedor: PROYECTO, REGION, BUCKET_RAW, BUCKET_CURATED, SA_SPARK.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator, BigQueryInsertJobOperator, BigQueryValueCheckOperator)
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.task_group import TaskGroup

PROYECTO = os.environ.get("PROYECTO", "tespipaber")
REGION = os.environ.get("REGION", "us-central1")
BUCKET_RAW = os.environ.get("BUCKET_RAW", f"{PROYECTO}-lumina-raw")
BUCKET_CURATED = os.environ.get("BUCKET_CURATED", f"{PROYECTO}-lumina-curated")
SA_SPARK = os.environ.get("SA_SPARK", f"lumina-spark@{PROYECTO}.iam.gserviceaccount.com")
UBICACION_BQ = "US"
CODIGO = f"gs://{BUCKET_CURATED}/_codigo/airflow"   # lo sube infra/07_vm_ops.sh
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Plantillas Jinja que dependen del parámetro `muestra`
DATASET = "{{ 'lumina_muestra' if params.muestra else 'lumina_curated' }}"
PREFIJO = f"gs://{BUCKET_CURATED}{{{{ '/muestra' if params.muestra else '' }}}}"
BLOQUES = "{{ 2 if params.muestra else 0 }}"
# Sincronización 1: silver consolida lo que el streaming copió a bronze (parámetro `streaming`)
BRONZE_STREAMING = f"{{{{ 'gs://{BUCKET_RAW}/streaming/lecturas' if params.streaming else '' }}}}"


def tabla_bq(nombre: str) -> str:
    # Interpolación y no str.format: format convertiría las llaves dobles de Jinja en simples.
    return f"`{PROYECTO}.{DATASET}.{nombre}`"

# Tablas gold que se cargan y su particionado/clustering en BigQuery
CARGAS: dict[str, dict] = {
    "dim_hogar": {}, "dim_fecha": {}, "dim_acorn": {}, "acorn_atributos": {},
    "fact_clima_hora": {}, "fact_clima_dia": {}, "fact_demanda_nacional_30min": {},
    "fact_carga_zona_30min": {}, "perfil_hogar": {}, "dq_medidor": {},
    "fact_consumo_30min": {"timePartitioning": {"type": "DAY", "field": "fecha"},
                           "clustering": {"fields": ["lclid"]}},
    "fact_consumo_diario": {"timePartitioning": {"type": "MONTH", "field": "fecha"},
                            "clustering": {"fields": ["lclid"]}},
    "fact_carga_transformador_30min": {"timePartitioning": {"type": "MONTH", "field": "fecha"},
                                       "clustering": {"fields": ["zona", "transformador"]}},
}


def batch_spark(job: str, entrada: str, salida: str, extra: list[str] | None = None) -> dict:
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"{CODIGO}/{job}.py",
            "python_file_uris": [f"{CODIGO}/lumina_common.py"],
            "args": ["--entrada", entrada, "--salida", salida, "--muestra-bloques", BLOQUES, *(extra or [])],
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {
                "spark.executor.instances": "2",
                "spark.dynamicAllocation.maxExecutors": "4",
                "spark.sql.session.timeZone": "UTC",
                "spark.sql.shuffle.partitions": "128",
            },
        },
        "environment_config": {"execution_config": {
            "service_account": SA_SPARK, "subnetwork_uri": "default", "ttl": "7200s"}},
        "labels": {"proyecto": "lumina", "curso": "bigdata-utec", "job": job, "orquestador": "airflow"},
    }


def sql(archivo: str) -> str:
    return (SQL_DIR / archivo).read_text(encoding="utf-8").replace("${P}", PROYECTO).replace("${DS}", DATASET)


def consulta(task_id: str, texto: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id, project_id=PROYECTO, location=UBICACION_BQ,
        configuration={"query": {"query": texto, "useLegacySql": False}})


with DAG(
    dag_id="lumina_batch_diario",
    description="Batch diario Lumina: Spark serverless (silver/gold) + BigQuery + calidad",
    # En el taller: sin programación (solo disparos manuales), porque al despausar el DAG Airflow
    # lanzaría por sí solo una corrida completa (~US$0.65). En producción: LUMINA_PROGRAMACION="0 6 * * *".
    schedule=os.environ.get("LUMINA_PROGRAMACION") or None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    # Reintentos con espera: un batch de Spark puede pedir CPU antes de que el anterior libere la cuota.
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    params={"muestra": Param(False, type="boolean",
                             description="2 bloques de medidores en lumina_muestra (prueba barata)"),
            "streaming": Param(True, type="boolean",
                               description="Consolidar en silver las lecturas que el streaming copió a bronze")},
    tags=["lumina", "batch", "fase-2"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    verificar_raw = GCSObjectsWithPrefixExistenceSensor(
        task_id="verificar_raw", bucket=BUCKET_RAW,
        prefix="kaggle/smart_meters_london/halfhourly_dataset/halfhourly_dataset/block_",
        mode="reschedule", poke_interval=60, timeout=600)

    silver = DataprocCreateBatchOperator(
        task_id="spark_silver", project_id=PROYECTO, region=REGION,
        batch_id="lumina-af-silver-{{ ts_nodash | lower }}-{{ ti.try_number }}",   # un id por intento
        batch=batch_spark("silver", f"gs://{BUCKET_RAW}/kaggle", f"{PREFIJO}/silver",
                          ["--entrada-streaming", BRONZE_STREAMING]))

    gold = DataprocCreateBatchOperator(
        task_id="spark_gold", project_id=PROYECTO, region=REGION,
        batch_id="lumina-af-gold-{{ ts_nodash | lower }}-{{ ti.try_number }}",
        batch=batch_spark("gold", f"{PREFIJO}/silver", f"{PREFIJO}/gold"))

    with TaskGroup("cargar_bigquery") as cargas:
        for tabla, extra in CARGAS.items():
            BigQueryInsertJobOperator(
                task_id=f"cargar_{tabla}", project_id=PROYECTO, location=UBICACION_BQ,
                configuration={"load": {
                    "sourceUris": [f"{PREFIJO}/gold/{tabla}/*.parquet"],
                    "sourceFormat": "PARQUET",
                    "destinationTable": {"projectId": PROYECTO, "datasetId": DATASET, "tableId": tabla},
                    "writeDisposition": "WRITE_TRUNCATE",
                    **extra,
                }})

    derivadas = consulta("tablas_derivadas", sql("gold_derivadas.sql"))

    with TaskGroup("calidad") as calidad:
        BigQueryValueCheckOperator(
            task_id="sin_duplicados", location=UBICACION_BQ, use_legacy_sql=False, pass_value=0,
            sql=f"SELECT COUNT(*) - COUNT(DISTINCT FORMAT('%s|%t', lclid, ts_utc)) "
                f"FROM {tabla_bq('fact_consumo_30min')}")
        BigQueryValueCheckOperator(
            task_id="diario_cuadra_con_30min", location=UBICACION_BQ, use_legacy_sql=False, pass_value=0,
            sql=f"""SELECT COUNT(*) FROM (
                      SELECT lclid, fecha, SUM(kwh) AS kwh FROM {tabla_bq('fact_consumo_30min')}
                      GROUP BY 1, 2) a
                    JOIN {tabla_bq('fact_consumo_diario')} d USING (lclid, fecha)
                    WHERE ABS(a.kwh - d.kwh_total) > 0.001""")
        BigQueryCheckOperator(
            task_id="tablas_completas", location=UBICACION_BQ, use_legacy_sql=False,
            sql=f"""SELECT
                      (SELECT COUNT(*) FROM {tabla_bq('fact_consumo_30min')}) > 0,
                      (SELECT COUNT(*) FROM {tabla_bq('consumo_dia_anidado')})
                        = (SELECT COUNT(*) FROM {tabla_bq('fact_consumo_diario')}),
                      (SELECT COUNT(*) FROM {tabla_bq('umbral_zona')}) > 0,
                      (SELECT COUNTIF(lclid IS NULL) FROM {tabla_bq('dim_hogar')}) = 0""")

    fin = EmptyOperator(task_id="fin")

    inicio >> verificar_raw >> silver >> gold >> cargas >> derivadas >> calidad >> fin
