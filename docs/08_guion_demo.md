# Guion de la demo end-to-end (Entrega 3)

Demo en vivo de unos 12 minutos para la sustentación del 21 de noviembre. Muestra un proceso de negocio completo, **decisiones en tiempo real durante una ola de frío**, de la lectura del medidor hasta la API y el tablero, y cierra con el batch y la analítica. Todos los comandos se probaron en la sesión en vivo del 3 de noviembre de 2026.

## Antes de la clase (30 minutos antes)

| Paso | Comando o acción | Para qué |
|---|---|---|
| 1 | `bash infra/07_vm_ops.sh crear streaming,airflow` | Enciende la VM con Kafka, Spark Streaming y Airflow (~8 min). Reinicia los *checkpoints* porque el Kafka es nuevo |
| 2 | `bash infra/07_vm_ops.sh refresco 0.05` | El streaming recarga las reglas del batch cada 3 min, para que se vea en la demo |
| 3 | `bash infra/07_vm_ops.sh logs spark-streaming` | Esperar la línea `[lumina] 5 consultas activas` |
| 4 | Abrir en el navegador: la API (`/docs`), el tablero de Looker Studio y la consola de BigQuery | Tener las pestañas listas |
| 5 | `bash infra/07_vm_ops.sh tunel` en otra terminal | Airflow en `http://localhost:8080` |

Costo de la demo: ≈ US$0.15 de VM por hora, más ≈ US$0.3 si se corre el DAG.

## Durante la demo

| Min | Qué se muestra | Comando o pantalla | Qué decir |
|---|---|---|---|
| 0–1 | El problema | Diapositiva del resumen ejecutivo | La hora punta es cara y las lecturas solo sirven para facturar |
| 1–3 | **Llegan las lecturas** | `bash infra/07_vm_ops.sh replay --desde 2013-01-14 --dias 2 --segundos-intervalo 1 --concentrador-caido Z3-T07 --caida-desde "'2013-01-14 06:00:00'" --anomalias 20` | Cada segundo llegan 30 minutos de lecturas de 5,508 hogares reales de Londres. A las 06:00 se cae un concentrador y se inyectan 20 consumos anómalos |
| 3–5 | **Alertas en segundos** | Consola de BigQuery: `SELECT tipo, COUNT(*) FROM lumina_rt.alertas WHERE ts_procesado > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE) GROUP BY tipo` | Aparecen los 52 medidores en silencio del concentrador Z3-T07, las anomalías y los picos de zona, estos últimos solo entre 17 y 20 h |
| 5–6 | **El batch enseña al streaming** | `bash infra/07_vm_ops.sh logs spark-streaming` (línea `reglas del batch cargadas`) | Los umbrales que usa el streaming vienen del batch y se recargan solos (sincronización 2) |
| 6–7 | **Lo de hoy pasa a la historia** | `gcloud storage ls gs://<proyecto>-lumina-raw/streaming/lecturas/` | Cada lectura en vivo también se guarda en el lake; el batch de la noche la vuelve definitiva (sincronización 1) |
| 7–8 | **La orquestación** | Airflow en `http://localhost:8080`: vista de grafo de `lumina_batch_diario` | Sensor, Spark silver y gold, 13 cargas a BigQuery y 3 controles de calidad |
| 8–10 | **La API** | `/docs` → `/alertas`, `/zonas/3/carga?fecha=2013-01-16`, `/zonas/3/pronostico?fecha=2014-01-15`, `/mantenimiento/transformadores` | Lo que consumen la app y otros sistemas, con una sola vista que une historia y hoy |
| 10–11 | **El tablero** | Looker Studio: páginas de operación, tarifa y mantenimiento | La tarifa dinámica ahorra 2.74 % y recorta 5.6 % la punta; el pronóstico se equivoca 4.4 % |
| 11–12 | Cierre | Diapositiva de costos | Todo esto costó ≈ US$2.3 de los US$300; en producción, ≈ US$36 k al año frente a ≈ US$9.6 M de beneficio |

## Si algo falla en vivo

| Síntoma | Qué hacer |
|---|---|
| No aparecen alertas a los 3 minutos | `bash infra/07_vm_ops.sh logs spark-streaming`. Si dice `offset was changed`, los *checkpoints* son de un Kafka anterior: `crear` los reinicia; si la VM ya existía, detener el contenedor, borrar `gs://<curated>/checkpoints/streaming` y volver a levantarlo |
| La VM no responde | Mostrar las alertas ya guardadas de esta prueba en BigQuery y en la API (`/alertas`): los datos persisten aunque la VM no esté |
| No hay internet estable | Mostrar las cifras medidas en la sesión del 3 de noviembre ([Entrega 2](07_entrega2.md)). Conviene grabar un video de respaldo durante el ensayo |

## Después de la demo

```bash
bash infra/07_vm_ops.sh destruir     # costo 0 entre demos
bash infra/shutdown.sh               # verifica que no quede nada encendido
```

La API en Cloud Run puede quedar publicada hasta la nota final (escala a cero, ~US$0). Al cerrar el curso: `bash infra/13_api_cloud_run.sh borrar`.
