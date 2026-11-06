#!/usr/bin/env bash
# Fase 4 · API de Lumina en Cloud Run: pública para la demo, máximo 1 instancia, escala a cero.
#
#   bash infra/13_api_cloud_run.sh desplegar    # construye la imagen (Cloud Build) y publica el servicio
#   bash infra/13_api_cloud_run.sh url          # muestra la URL pública
#   bash infra/13_api_cloud_run.sh borrar       # borra el servicio y sus imágenes (costo 0)
#
# Costo: ~US$0. Cloud Run cobra solo con peticiones (capa gratuita de 2 M al mes); cada consulta tiene
# un tope de 2 GiB facturados en BigQuery y la API no puede pasar de 1 instancia, así que un abuso
# queda acotado. La cuenta de servicio solo lee los datasets que la API usa.
source "$(dirname "$0")/_common.sh"
require_config

SERVICIO="lumina-api"
SA_NAME="lumina-api"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REPO_AR="cloud-run-source-deploy"

lector_dataset() {  # $1 dataset: lectura solo en ese dataset (condición IAM)
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:$SA_EMAIL" --role roles/bigquery.dataViewer \
    --condition "title=lee_$1,expression=resource.name.startsWith(\"projects/$PROJECT_ID/datasets/$1\")"     --quiet >/dev/null </dev/null   # sin entrada: gcloud no puede quedarse esperando una respuesta
}

case "${1:-}" in
  desplegar)
    log "APIs de Cloud Run, Cloud Build y Artifact Registry"
    gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
      --project "$PROJECT_ID"

    log "Cuenta de servicio $SA_EMAIL (solo lectura)"
    if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
      gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" --display-name "Lumina · API en Cloud Run"
      sleep 10
    fi
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
      --member "serviceAccount:$SA_EMAIL" --role roles/bigquery.jobUser --quiet >/dev/null </dev/null
    # Las vistas de lumina_olap leen curated y rt: el lector necesita los tres, más lumina_ml.
    for ds in lumina_olap lumina_ml lumina_curated lumina_rt; do lector_dataset "$ds"; done

    log "Construyendo y publicando $SERVICIO (pública, máx. 1 instancia)"
    gcloud run deploy "$SERVICIO" --project "$PROJECT_ID" --region "$REGION" \
      --source "$REPO_DIR/src/api" --quiet \
      --service-account "$SA_EMAIL" \
      --allow-unauthenticated \
      --min-instances 0 --max-instances 1 --concurrency 20 \
      --cpu 1 --memory 512Mi --timeout 60 \
      --set-env-vars "PROYECTO=$PROJECT_ID" \
      --labels "$LABELS"
    "$0" url
    ;;

  url)
    url="$(gcloud run services describe "$SERVICIO" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')"
    echo "API: $url   ·   documentación interactiva: $url/docs"
    ;;

  borrar)
    confirm "Se borra el servicio $SERVICIO y sus imágenes."
    gcloud run services delete "$SERVICIO" --project "$PROJECT_ID" --region "$REGION" --quiet || true
    gcloud artifacts repositories delete "$REPO_AR" --project "$PROJECT_ID" --location "$REGION" --quiet || true
    ;;

  *) die "Uso: 13_api_cloud_run.sh desplegar|url|borrar" ;;
esac
