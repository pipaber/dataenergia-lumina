#!/usr/bin/env bash
# VM lumina-ops: Docker Compose con perfiles `streaming` (Kafka + Spark Structured Streaming)
# y `airflow` (orquesta el batch en Spark serverless). Solo se enciende para probar o demostrar.
#
#   bash infra/07_vm_ops.sh tablas                 # tablas de salida lumina_rt (sin VM)
#   bash infra/07_vm_ops.sh crear [perfiles]       # crea y ENCIENDE la VM; perfiles: streaming (def.), airflow o streaming,airflow
#   bash infra/07_vm_ops.sh perfiles <lista>       # cambia los perfiles activos en la VM encendida
#   bash infra/07_vm_ops.sh desplegar              # sube el código y reinicia el stack
#   bash infra/07_vm_ops.sh refresco <horas>       # cada cuánto el streaming recarga reglas (def. 24; demo: 0.05)
#   bash infra/07_vm_ops.sh iniciar | detener      # enciende / apaga (el disco cuesta ~US$3/mes)
#   bash infra/07_vm_ops.sh destruir               # BORRA VM y disco (costo 0 entre demos)
#   bash infra/07_vm_ops.sh replay [args]          # p. ej. --dias 2 --anomalias 20
#   bash infra/07_vm_ops.sh dag [muestra]          # dispara lumina_batch_diario (muestra = 2 bloques)
#   bash infra/07_vm_ops.sh tunel                  # Airflow en http://localhost:8080 (Ctrl+C para cerrar)
#   bash infra/07_vm_ops.sh clave_airflow          # muestra la contraseña del usuario admin de Airflow
#   bash infra/07_vm_ops.sh logs [servicio] | estado
#
# Costo: e2-standard-4 US$0.134/h encendida. Red de seguridad: horario que la apaga a las 23:00 (Lima).
source "$(dirname "$0")/_common.sh"
require_config

SA_NAME="lumina-ops"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
SA_SPARK="lumina-spark@$PROJECT_ID.iam.gserviceaccount.com"
HORARIO="lumina-apagado-nocturno"
VM=(--project "$PROJECT_ID" --zone "$ZONE")
COMPOSE="cd /opt/lumina/infra/ops && sudo docker compose"

ssh_vm() { gcloud compute ssh "$OPS_VM" "${VM[@]}" --quiet --command "$1"; }

subir_bundle() {
  local tmp; tmp="$(mktemp -d)"
  tar -czf "$tmp/bundle.tgz" -C "$REPO_DIR" infra/ops src/streaming src/dags src/sql
  gcloud storage cp "$tmp/bundle.tgz" "gs://$BUCKET_CURATED/_ops/bundle.tgz" --quiet
  # Código de los jobs batch que lanza Airflow en Spark serverless
  gcloud storage cp "$REPO_DIR"/src/batch/{silver,gold,lumina_common}.py \
    "gs://$BUCKET_CURATED/_codigo/airflow/" --quiet
  rm -rf "$tmp"
}

dataset_editor() {  # $1 dataset: escritura solo en ese dataset (condición IAM)
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:$SA_EMAIL" --role roles/bigquery.dataEditor \
    --condition "title=solo_$1,expression=resource.name.startsWith(\"projects/$PROJECT_ID/datasets/$1\")" >/dev/null
}

permisos() {
  log "Cuenta de servicio $SA_EMAIL y permisos de mínimo privilegio"
  if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" \
      --display-name "Lumina · VM de streaming y Airflow"
    sleep 10
  fi
  for rol in roles/bigquery.jobUser roles/bigquery.readSessionUser roles/bigquery.dataViewer \
             roles/logging.logWriter roles/dataproc.editor; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
      --member "serviceAccount:$SA_EMAIL" --role "$rol" >/dev/null
  done
  for ds in lumina_rt lumina_curated lumina_muestra; do dataset_editor "$ds"; done
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_CURATED" \
    --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectAdmin >/dev/null
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_RAW" \
    --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectViewer >/dev/null
  # Sincronización 1: el streaming escribe la copia cruda solo bajo raw/streaming/; el resto de raw es de solo lectura.
  # gcloud no agrega bindings condicionales a una política v1, así que se edita la política (v3) como JSON.
  local pol; pol="$(mktemp)"
  gcloud storage buckets get-iam-policy "gs://$BUCKET_RAW" --format=json > "$pol"
  python -E -X utf8 - "$pol" "$SA_EMAIL" "$BUCKET_RAW" <<'PY'
import json, sys
ruta, sa, bucket = sys.argv[1:]
pol = json.load(open(ruta))
titulo = 'solo_streaming'
if not any((b.get('condition') or {}).get('title') == titulo for b in pol.get('bindings', [])):
    pol.setdefault('bindings', []).append({
        'role': 'roles/storage.objectAdmin', 'members': [f'serviceAccount:{sa}'],
        'condition': {'title': titulo,
                      'expression': f'resource.name.startsWith("projects/_/buckets/{bucket}/objects/streaming/")'}})
pol['version'] = 3
json.dump(pol, open(ruta, 'w'))
PY
  gcloud storage buckets set-iam-policy "gs://$BUCKET_RAW" "$pol" >/dev/null
  rm -f "$pol"
  # Airflow lanza los batches de Spark como lumina-spark
  gcloud iam service-accounts add-iam-policy-binding "$SA_SPARK" --project "$PROJECT_ID" \
    --member "serviceAccount:$SA_EMAIL" --role roles/iam.serviceAccountUser >/dev/null
}

case "${1:-}" in
  tablas)
    sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/streaming_tablas.sql" |
      bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql
    ;;

  crear)
    PERFILES="${2:-streaming}"
    permisos

    log "Horario $HORARIO: apaga la VM a las 23:00 (America/Lima)"
    if ! gcloud compute resource-policies describe "$HORARIO" --project "$PROJECT_ID" --region "$REGION" >/dev/null 2>&1; then
      gcloud compute resource-policies create instance-schedule "$HORARIO" \
        --project "$PROJECT_ID" --region "$REGION" \
        --vm-stop-schedule "0 23 * * *" --timezone "America/Lima"
      # El agente de servicio de Compute necesita permiso para detener la VM.
      numero="$(gcloud projects describe "$PROJECT_ID" --format 'value(projectNumber)')"
      gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
        --member "serviceAccount:service-$numero@compute-system.iam.gserviceaccount.com" \
        --role roles/compute.instanceAdmin.v1 >/dev/null
    fi

    # Kafka nace vacío en cada VM: los checkpoints del streaming guardan offsets del Kafka anterior y Spark
    # se detendría ("offset was changed ... some data may have been missed"). Se reinician junto con el
    # registro de commits de la copia a bronze; los Parquet ya escritos se conservan y silver deduplica.
    log "Reiniciando checkpoints del streaming (el Kafka de la VM nueva empieza en el offset 0)"
    gcloud storage rm -r "gs://$BUCKET_CURATED/checkpoints/streaming" --quiet 2>/dev/null || true
    gcloud storage rm -r "gs://$BUCKET_RAW/streaming/lecturas/_spark_metadata" --quiet 2>/dev/null || true

    log "Subiendo código y creando $OPS_VM con perfiles '$PERFILES' (queda ENCENDIDA)"
    subir_bundle
    gcloud compute instances create "$OPS_VM" "${VM[@]}" \
      --machine-type e2-standard-4 \
      --image-family debian-12 --image-project debian-cloud \
      --boot-disk-size 30GB --boot-disk-type pd-balanced \
      --service-account "$SA_EMAIL" --scopes cloud-platform \
      --resource-policies "$HORARIO" \
      --labels "$LABELS" \
      --metadata "^;^project-id=$PROJECT_ID;bucket-curated=$BUCKET_CURATED;bucket-raw=$BUCKET_RAW;region=$REGION;sa-spark=$SA_SPARK;perfiles=$PERFILES" \
      --metadata-from-file "startup-script=$REPO_DIR/infra/ops/vm_startup.sh"
    echo "El primer arranque instala Docker y levanta el stack (~5-8 min): bash infra/07_vm_ops.sh logs"
    ;;

  perfiles)
    [[ -n "${2:-}" ]] || die "Uso: 07_vm_ops.sh perfiles streaming|airflow|streaming,airflow"
    gcloud compute instances add-metadata "$OPS_VM" "${VM[@]}" --metadata "^;^perfiles=$2"
    ssh_vm "sudo google_metadata_script_runner startup"
    ;;

  refresco)
    [[ -n "${2:-}" ]] || die "Uso: 07_vm_ops.sh refresco <horas>"
    gcloud compute instances add-metadata "$OPS_VM" "${VM[@]}" --metadata "refresco-horas=$2"
    ssh_vm "sudo google_metadata_script_runner startup"
    ;;

  desplegar)
    subir_bundle
    ssh_vm "sudo google_metadata_script_runner startup"
    ;;

  iniciar)
    gcloud compute instances start "$OPS_VM" "${VM[@]}"
    ;;

  replay)
    shift
    ssh_vm "cd /opt/lumina/infra/ops && sudo COMPOSE_PROFILES=streaming,replay docker compose run --rm replay --proyecto $PROJECT_ID $*"
    ;;

  dag)
    conf='{}'
    [[ "${2:-}" == "muestra" ]] && conf='{"muestra": true}'
    ssh_vm "$COMPOSE exec -T airflow-scheduler airflow dags unpause lumina_batch_diario >/dev/null && \
            $COMPOSE exec -T airflow-scheduler airflow dags trigger lumina_batch_diario --conf '$conf'"
    ;;

  tunel)
    echo "Airflow en http://localhost:8080 (usuario admin; clave: bash infra/07_vm_ops.sh clave_airflow)."
    echo "Deja esta ventana abierta; Ctrl+C cierra el túnel."
    gcloud compute ssh "$OPS_VM" "${VM[@]}" --quiet --ssh-flag="-L 8080:localhost:8080" --ssh-flag="-N"
    ;;

  clave_airflow)
    ssh_vm "sudo cat /opt/lumina/.airflow_admin"
    ;;

  logs)
    ssh_vm "$COMPOSE logs --tail 80 ${2:-} 2>/dev/null || sudo tail -n 60 /var/log/lumina-ops.log"
    ;;

  estado)
    gcloud compute instances describe "$OPS_VM" "${VM[@]}" --format "value(name,status)"
    ssh_vm "$COMPOSE ps" || true
    ;;

  detener)
    gcloud compute instances stop "$OPS_VM" "${VM[@]}" --quiet
    ;;

  destruir)
    # Borra VM y disco: costo 0 entre demos. Todo el estado durable está en GCS/BigQuery
    # y "crear" reconstruye el stack (~8 min). Conserva la cuenta de servicio y el horario.
    gcloud compute instances delete "$OPS_VM" "${VM[@]}" --quiet
    ;;

  *)
    sed -n '2,17p' "$0"; exit 1 ;;
esac
