#!/usr/bin/env bash
# BORRA todos los recursos del proyecto Lumina (fin del curso). Irreversible.
# Conserva solo el presupuesto (no cobra). Uso: bash infra/teardown.sh
source "$(dirname "$0")/_common.sh"
require_config

cat <<EOF
Se borrará en el proyecto $PROJECT_ID:
  - VMs: $INGEST_VM, $OPS_VM (y sus discos)
  - Servicio de Cloud Run lumina-api y sus imágenes
  - Buckets: gs://$BUCKET_RAW, gs://$BUCKET_CURATED y los dataproc-staging/temp (con todo su contenido)
  - Datasets BigQuery: $BQ_DATASETS (con todas sus tablas)
  - Secreto: $KAGGLE_SECRET
  - Cuentas de servicio lumina-*
EOF
confirm "¿Borrar todo?"

bash "$INFRA_DIR/shutdown.sh"

for vm in "$INGEST_VM" "$OPS_VM"; do
  gcloud compute instances delete "$vm" --zone "$ZONE" --project "$PROJECT_ID" --quiet 2>/dev/null \
    && echo "VM $vm borrada" || true
done

gcloud run services delete lumina-api --region "$REGION" --project "$PROJECT_ID" --quiet 2>/dev/null   && echo "Servicio lumina-api borrado" || true
gcloud artifacts repositories delete cloud-run-source-deploy --location "$REGION" --project "$PROJECT_ID" --quiet 2>/dev/null   && echo "Imágenes de Cloud Run borradas" || true

# Incluye los buckets de staging/temp que Dataproc Serverless crea solo en $REGION.
# OJO: los dataproc-*-us-east1-* del proyecto son de labs anteriores (sep-2026) y NO se tocan.
for bucket in "$BUCKET_RAW" "$BUCKET_CURATED" $(gcloud storage ls --project "$PROJECT_ID" 2>/dev/null \
    | sed -n "s#^gs://\(dataproc-\(staging\|temp\)-$REGION-[^/]*\)/\$#\1#p"); do
  gcloud storage rm -r "gs://$bucket" --project "$PROJECT_ID" --quiet 2>/dev/null \
    && echo "Bucket $bucket borrado" || true
done

for ds in $BQ_DATASETS; do
  bq --project_id "$PROJECT_ID" rm -r -f --dataset "$PROJECT_ID:$ds" 2>/dev/null \
    && echo "Dataset $ds borrado" || true
done

gcloud secrets delete "$KAGGLE_SECRET" --project "$PROJECT_ID" --quiet 2>/dev/null || true

for sa in $(gcloud iam service-accounts list --project "$PROJECT_ID" \
    --filter "email~^lumina-" --format "value(email)"); do
  gcloud iam service-accounts delete "$sa" --project "$PROJECT_ID" --quiet
done

log "Teardown completo. Revisa la facturación en 24-48 h para confirmar costo 0."
