---
title: "DataEnergía Metropolitan · Lumina Energía"
subtitle: "Entrega 2: pipelines, procesamiento híbrido y modelado"
author: "Piero Palacios"
date: 2026-11-07
lang: es
---


> Documento elaborado con asistencia de IA generativa (Claude). Las cifras salen de corridas reales en Google Cloud; los supuestos están declarados en cada sección.

Esta entrega demuestra la capa de procesamiento: el código que limpia, cruza y modela los datos, y su funcionamiento en los dos caminos, por lotes (*batch*) y en tiempo real (*streaming*). Todo el código está en el repositorio <https://github.com/pipaber/dataenergia-lumina>. Las pruebas en vivo se hicieron el 3 de noviembre de 2026 sobre Google Cloud.

## Qué pide la entrega y dónde se cumple

| Requisito | Cómo se cumple | Código |
|---|---|---|
| **Pipelines de cómputo y transformación**: limpieza, enriquecimiento y cruce de datos heterogéneos | Spark limpia 167.8 M lecturas (texto con espacios, nulos, lecturas fuera de la grilla de 30 min, duplicados), las convierte a UTC y las cruza con hogares, segmentos Acorn, clima, feriados y demanda nacional | [`src/batch/silver.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/batch/silver.py), [`src/batch/gold.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/batch/gold.py), [`src/sql/gold_derivadas.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/gold_derivadas.sql) |
| **Procesamiento híbrido**: tiempo real y lotes | Camino batch con Spark serverless orquestado por Airflow; camino en tiempo real con Kafka y Spark Structured Streaming; ambos se sincronizan con el patrón Lambda | [`src/streaming/`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/streaming/streaming_job.py), [`src/dags/lumina_batch_diario.py`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/dags/lumina_batch_diario.py) |
| **Modelado para analítica**: estructuras finales listas para el consumo | Modelo estrella en BigQuery (2 dimensiones de hogar y fecha, 6 tablas de hechos), una tabla anidada con `STRUCT`/`ARRAY`, perfiles por hogar, diagnóstico por medidor y vistas OLAP que unen historia y hoy | [`docs/02_modelo_datos.md`](https://pipaber.github.io/dataenergia-lumina/docs/02_modelo_datos.html), [`src/sql/olap_vistas.sql`](https://github.com/pipaber/dataenergia-lumina/blob/main/src/sql/olap_vistas.sql) |

## 1 · Pipeline batch

El camino batch procesa la historia completa una vez al día. Sigue las capas del lake:

| Capa | Qué hace | Dónde vive |
|---|---|---|
| raw (*bronze*) | Copia fiel de Kaggle y, desde el streaming, de cada lectura en vivo | `gs://<proyecto>-lumina-raw/` |
| silver | Datos limpios y tipados, en UTC, con banderas de calidad | `gs://<proyecto>-lumina-curated/silver/` |
| gold | Modelo estrella, agregados, perfiles por hogar y diagnóstico de cada medidor | `gs://<proyecto>-lumina-curated/gold/` |
| warehouse | Las tablas gold cargadas en BigQuery, más el clima público de NOAA | Dataset `lumina_curated` |

**Validación.** La corrida completa procesó 167,811,461 lecturas. Los 3,510,403 pares medidor-día coinciden exactamente con el agregado diario que publica Kaggle (35.54 GWh en ambos lados). Silver tardó 10.6 min y gold 21.8 min en Spark serverless, por ≈ US$0.65 en total.

## 2 · Orquestación con Airflow

Airflow encadena el batch en el DAG `lumina_batch_diario`: espera los datos en raw, lanza silver y gold en Spark serverless, carga 13 tablas en BigQuery en paralelo, recalcula las tablas derivadas y corre 3 controles de calidad. Detalle en [Orquestación con Airflow](https://pipaber.github.io/dataenergia-lumina/docs/04_orquestacion_airflow.html).

**Corrida del 3 de noviembre de 2026 (modo muestra).** El DAG terminó **con las 22 tareas en verde**: sensor, silver y gold en Spark serverless, las 13 cargas a BigQuery, las tablas derivadas y los 3 controles de calidad (sin duplicados, el diario cuadra con el detalle de 30 min y tablas completas). Silver tardó 3.4 min y gold 3.9 min. El primer intento de gold volvió a chocar con la cuota de CPU mientras Google liberaba las máquinas de silver; el reintento automático, con un nombre de batch nuevo, terminó bien sin intervención.

## 3 · Pipeline en tiempo real

Las lecturas llegan a Kafka y Spark Structured Streaming las evalúa en cinco consultas:

| Consulta | Qué detecta o guarda |
|---|---|
| `anomalias` | Consumo muy por encima del perfil del propio hogar a esa hora |
| `carga_zona` | Picos de carga por zona, solo en hora punta (17–20 h) |
| `silencio` | Medidores que dejan de enviar (2 h sin lecturas) |
| `demanda` | Demanda nacional de la red |
| `bronze` | Copia cruda de cada lectura en el lake, para el batch |

**Prueba en vivo del 3 de noviembre.** Se reprodujeron 2 días de la ola de frío de enero de 2013: 523,868 lecturas de 5,508 hogares reales, con un concentrador caído desde las 06:00 y 20 consumos anómalos inyectados.

| Qué se verificó | Resultado |
|---|---|
| Concentrador caído (Z3-T07) | Sus **52 medidores** generaron la alerta `medidor_silencioso` desde su última lectura (05:30); 5 alertas más corresponden a huecos reales de otros medidores |
| Anomalías inyectadas | Las 5 que se revisaron una por una fueron detectadas; por ejemplo, 20.65 kWh frente a un umbral de 6.43 kWh para ese hogar a esa hora. En la sesión de octubre se detectaron las 20 de 20. En total hubo 109 alertas de consumo anómalo en 78 hogares |
| Picos de zona | **33 picos**, todos entre las 17 y las 20 h, como exige la regla de hora punta por estación |
| Carga por zona | 744 filas: 8 zonas × 93 medias horas |
| Latencia | Mediana de 43 s de Kafka a BigQuery para las anomalías, con el replay a 1,800 veces la velocidad real (1 s = 30 min) |

## 4 · Sincronización entre batch y streaming (patrón Lambda)

La arquitectura combina ambos caminos con cuatro puntos de sincronización. Los cuatro quedaron probados:

| # | Sincronización | Evidencia |
|---|---|---|
| 1 | La lectura en vivo pasa a la historia | La consulta `bronze` escribió en el lake exactamente las **523,868 lecturas de 5,508 medidores** del replay, en Parquet particionado por fecha. En la corrida del DAG, silver las consolidó: la muestra cubre normalmente 100 medidores, pero en los días del replay (14 y 15 de enero de 2013) tiene **5,508 medidores y 523,868 lecturas, con 0 duplicados** |
| 2 | Lo aprendido por el batch llega al streaming | Con la recarga en 3 min (`07_vm_ops.sh refresco 0.05`), el log mostró `reglas del batch cargadas: 5,566 hogares, 533,804 perfiles, 3,072 umbrales` al arrancar y de nuevo durante el replay. En producción el intervalo es de 24 h |
| 3 | Una sola vista para el consumo | La vista `lumina_olap.v_carga_zona_30min` une el batch y el streaming; la API la expone en `/zonas/{zona}/carga` |
| 4 | Manda el batch | La vista toma del streaming solo lo posterior al último dato del batch. El replay de 2013 cae dentro de la historia, así que aporta 0 filas: el dato definitivo es el del batch |

## 5 · Modelado para analítica

| Estructura | Para qué |
|---|---|
| Modelo estrella: `dim_hogar`, `dim_fecha`, `dim_acorn` y hechos de consumo (30 min y diario), carga por transformador y zona, demanda nacional y clima | Análisis OLAP por hogar, segmento, zona y fecha |
| `consumo_dia_anidado` con `STRUCT`/`ARRAY` | Una fila por medidor-día con sus 48 lecturas; lee 48 veces menos datos en el análisis por segmento |
| `perfil_hogar` y `umbral_zona` | Lo que el batch aprende y el streaming usa como regla |
| `dq_medidor` | Diagnóstico de calidad de cada medidor |
| Vistas `lumina_olap` (5) | La capa de servicio del patrón Lambda, para el tablero y la API |

Sobre estas tablas ya corren el pronóstico D+1, el análisis de tarifas y el mantenimiento predictivo de la Entrega 3: ver [Analítica](https://pipaber.github.io/dataenergia-lumina/docs/05_analitica.html).

## Problemas que encontró la prueba en vivo

La prueba en vivo sirvió para lo que debe servir: encontró cinco fallas que las pruebas sin VM no podían mostrar. Todas quedaron corregidas en el código.

| Problema | Causa | Corrección |
|---|---|---|
| La copia a bronze fallaba con `403 Forbidden` | La VM solo tenía permiso de lectura sobre la capa raw | Permiso de escritura **solo bajo `raw/streaming/`**, con una condición de IAM; el resto de raw sigue siendo de solo lectura |
| Las consultas se detenían con `offset was changed from 263696 to 0` | Los *checkpoints* guardaban posiciones del Kafka de la sesión anterior, y cada VM nueva trae un Kafka vacío | `07_vm_ops.sh crear` reinicia los *checkpoints* del streaming; los Parquet ya escritos se conservan y silver elimina duplicados |
| Gold no arrancaba: `Insufficient CPUS_ALL_REGIONS quota. Requested 12, available 8` | Gold pidió máquinas antes de que Google liberara las de silver; el reintento falló porque reusaba el mismo nombre de batch | Cada intento lleva su número en el nombre del batch, con 2 reintentos separados por 3 minutos |
| Los 3 controles de calidad fallaban con `Invalid dataset ID` | `str.format()` convertía las llaves dobles de la plantilla de Airflow en simples | El nombre de la tabla se arma con interpolación, sin `format` |
| El modo muestra no consolidaba la copia del streaming | El DAG le pasaba a silver una ruta vacía cuando `muestra` era verdadero | Nuevo parámetro `streaming` (activo por defecto), independiente de `muestra` |

## Cómo reproducirlo

```bash
bash infra/07_vm_ops.sh crear streaming,airflow    # ~8 min; reinicia los checkpoints
bash infra/07_vm_ops.sh refresco 0.05              # recarga de reglas cada 3 min (solo para la demo)
bash infra/07_vm_ops.sh replay --desde 2013-01-14 --dias 2 --segundos-intervalo 1 \
  --concentrador-caido Z3-T07 --caida-desde "'2013-01-14 06:00:00'" --anomalias 20
bash infra/07_vm_ops.sh dag muestra                # silver + gold + cargas + controles
bash infra/07_vm_ops.sh destruir                   # costo 0 al terminar
```

## Costo

| Recurso | Uso | Costo |
|---|---|---|
| VM `lumina-ops` (e2-standard-4) | 70 min, destruida al terminar | ≤ US$0.16 |
| Spark serverless (2 corridas del DAG en modo muestra) | 13.7 min | ≤ US$0.27 |
| BigQuery (cargas y consultas de verificación) | Las cargas no se cobran; las consultas leyeron pocos MB | ≈ US$0 |
| **Total de la sesión** | | **≤ US$0.43** |

Costo acumulado del proyecto: ≈ US$2.3 de US$300. Al terminar no quedó nada encendido salvo la API en Cloud Run, que escala a cero.
