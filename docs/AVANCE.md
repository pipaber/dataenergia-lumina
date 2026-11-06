# Avance del proyecto · DataEnergía / Lumina

Leyenda: ✅ hecho y verificado · 🟡 parcial o bloqueado · ⬜ pendiente

**Actualizado:** 2026-11-03 · **Costo acumulado:** ≈ US$2.3 de US$300 · **Recursos encendidos:** solo la API en Cloud Run (escala a cero)

## Resumen por entrega

| Entrega | Fecha | Estado | Falta |
|---|---|---|---|
| 1 · Sustentación + blueprint | 24-oct-2026 | ✅ | Entregada |
| 2 · Pipelines y modelado | 07-nov-2026 | ✅ | Documento listo ([07_entrega2.md](07_entrega2.md)); falta entregarlo (alumno) |
| 3 · API, demo y sustentación | 20/21-nov-2026 | 🟡 | Armar el tablero de Looker Studio (alumno, con la guía) y ensayar la demo; API, mantenimiento, guion y slides listos |

## Hitos

### Fase 0 · Plan
- ✅ Arquitectura propuesta, componentes y costo estimado → [00_plan_arquitectura.md](00_plan_arquitectura.md)
- ✅ Diagrama de servicios del taller → [diagrama_arquitectura.png](diagrama_arquitectura.png)

### Fase 1 · Setup en GCP
- ✅ APIs y presupuesto S/1,125 (≈ US$300) con alertas → `infra/01_apis_presupuesto.sh`
- ✅ Buckets raw/curated y datasets BigQuery → `infra/02_buckets_bigquery.sh`
- ✅ Token de Kaggle en Secret Manager → `infra/03_secreto_kaggle.sh`
- ✅ Ingesta Kaggle → raw (9.63 GiB) con VM efímera → `infra/04_ingesta_kaggle.sh`

### Fase 2 · Batch (camino batch)
- ✅ Limpieza bronze → silver (167.8 M lecturas) → `src/batch/silver.py`
- ✅ Modelo estrella, perfiles y calidad de medidores → `src/batch/gold.py`
- ✅ Carga a BigQuery (15 tablas) + tabla con STRUCT/ARRAY → `infra/06_cargar_bigquery.sh`
- ✅ Validación exacta contra Kaggle (3,510,403 medidor-día, 35.54 GWh) → [02_modelo_datos.md](02_modelo_datos.md)

### Entrega 1 · Documento
- ✅ Sustentación técnica, económica y DRP/HA → [entrega1/sustentacion_entrega1.docx](entrega1/sustentacion_entrega1.docx)
- ✅ Diagrama de producción en 4 capas, con capa batch, capa de velocidad y vistas unificadas → [entrega1/diagrama_capas.png](entrega1/diagrama_capas.png)
- ✅ Sección "Patrón Lambda" con los 4 puntos de sincronización (§ 2.1 del documento)
- ✅ Calculadora de consumo del primer año → `src/costos/calculadora_consumo.py`
- ✅ Términos y siglas definidos en su primer uso + Glosario (§ 13)
- ✅ Versión para comité ejecutivo (comentario de la profesora): resumen con decisión, inversión vs. beneficio, riesgos y hoja de ruta; DRP con regiones us-central1/us-east1, política de respaldos, estrategia y costo de la resiliencia (+US$6.9 k/año), runbook con roles y simulacros → [entrega1/diagrama_regiones.png](entrega1/diagrama_regiones.png)
- ✅ Entregada (24-oct)

### Fase 3 · Streaming (camino tiempo real)
- ✅ Kafka + Spark Structured Streaming en la VM `lumina-ops` → `src/streaming/`, `infra/ops/`
- ✅ Replay de la ola de frío: 1.84 M lecturas, 20/20 anomalías y 52/52 medidores silenciosos detectados → [03_streaming.md](03_streaming.md)
- ✅ Umbrales calibrados offline (anomalías y picos por estación en hora punta)
- ✅ Regla de picos por estación validada en vivo (3-nov): 33 picos, todos entre 17 y 20 h
- ✅ Prueba en vivo del 3-nov: 523,868 lecturas, 52/52 medidores del concentrador caído, anomalías inyectadas detectadas → [07_entrega2.md](07_entrega2.md)

### Sincronización batch ↔ streaming (arquitectura Lambda)
- ✅ Patrón definido y documentado (Lambda, no Kappa), con diagrama → [arquitectura_tecnica.md](arquitectura_tecnica.md#batch-y-streaming-arquitectura-lambda)
- ✅ 1 · Lectura en vivo → bronze → silver: probado en vivo; la muestra tiene los 5,508 medidores del replay en sus 2 días, con 0 duplicados
- ✅ 2 · Perfiles y umbrales → streaming: recarga probada en vivo con intervalo de 3 min (`07_vm_ops.sh refresco 0.05`)
- ✅ 3 · Vistas que unen historia (`curated`) y hoy (`rt`) en `lumina_olap` → `src/sql/olap_vistas.sql`
- ✅ 4 · Reconciliación: de `rt` solo lo posterior al último batch; manda el batch (regla en `v_carga_zona_30min`)

### Orquestación · Airflow (camino batch, no tiempo real)
- ✅ DAG `lumina_batch_diario`: sensor → silver → gold → 13 cargas → derivadas → 3 controles → `src/dags/`
- ✅ Airflow en la VM, el DAG carga sin errores y el sensor pasa → [04_orquestacion_airflow.md](04_orquestacion_airflow.md)
- ✅ Cuota `CPUS_ALL_REGIONS` subida de 12 a 24 vCPU (alcanza para VM 4 + Spark serverless 16)
- ✅ Corrida del DAG con Spark (3-nov): 22/22 tareas en verde, con el reintento automático por cuota funcionando

### Fase 4 · Analítica (para la Entrega 3)
Detalle y resultados → [05_analitica.md](05_analitica.md)
- ✅ Calendario oficial dToU 2013 (17,520 medias horas) en `dim_tarifa_dtou` → `infra/08_tarifas_dtou.sh`
- ✅ Vistas OLAP en `lumina_olap` (5 vistas) → `infra/09_vistas_olap.sh`
- ✅ Tarifa dinámica vs. plana: −2.74 % de factura, 86 % de hogares ToU ahorran; −5.6 % de consumo en eventos Altos contra control → `infra/11_tarifas.sh`
- ✅ Pronóstico D+1 por zona (MAPE 4.4 % vs. 6.9 % ingenuo) y por hogar (WAPE 18.5 % vs. 21.3 %), ≤ US$0.42 → `infra/10_pronostico.sh`
- ✅ Mantenimiento predictivo: modelo de fallas (AUC 0.82; con el 5 % de inspecciones, 62 % de las fallas) y 14/112 transformadores con sobrecarga recurrente → `infra/12_mantenimiento.sh`
- ✅ App del cliente en Cloud Run → https://lumina-api-csascndwia-uc.a.run.app/app (un objetivo: pagar menos; 4 pestañas)
- ✅ Consola interna en Cloud Run → https://lumina-api-csascndwia-uc.a.run.app/consola (6 vistas, una por rol)
- ⬜ Opcional: tablero en Looker Studio con la [guía](06_guia_looker_studio.md); la consola ya cubre ese uso

### Entrega 3 · Exposición y demo
- ✅ API REST publicada en Cloud Run (pública, máx. 1 instancia): https://lumina-api-csascndwia-uc.a.run.app/docs → `infra/13_api_cloud_run.sh`
- ✅ Guion de demo end-to-end → [08_guion_demo.md](08_guion_demo.md)
- ✅ Slides con los 10 puntos del PDF (14 diapositivas, artifact privado; descargable como PowerPoint)
- ⬜ Ensayo de la demo (alumno) y entrega del repositorio (20-nov)
- ✅ Libro del proyecto (Quarto Book): 19 capítulos en 3 partes + apéndices, Parte I generada desde la sustentación, flujo interactivo incluido → `_quarto.yml`, `book/`
- ✅ Workflow de GitHub Pages → `.github/workflows/libro.yml`
- ✅ Enunciado del curso fuera del repo (copia en `bigdata utec/`)
- ✅ Historial limpio: el enunciado no está en ningún commit publicado (respaldo local en `backup/antes-de-quitar-enunciado`)
- ✅ Repositorio público → https://github.com/pipaber/dataenergia-lumina · libro → https://pipaber.github.io/dataenergia-lumina/ (se republica en cada push a `main`)

### Transversal
- ✅ Repositorio con commits por avance; secretos fuera de git
- ✅ Apagado: `infra/shutdown.sh`, `07_vm_ops.sh destruir`, horario 23:00, `teardown.sh`
- ✅ Bitácora de costos → [bitacora_costos.log](bitacora_costos.log)
- ✅ Arquitectura técnica (script → dónde corre → para qué) → [arquitectura_tecnica.md](arquitectura_tecnica.md)
- ✅ Ejemplo dummy del flujo de datos, ejecutable con pandas → `notebooks/flujo_dummy.py`
