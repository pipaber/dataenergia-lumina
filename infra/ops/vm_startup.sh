#!/usr/bin/env bash
# Startup script de la VM lumina-ops (corre como root en CADA arranque).
# 1) Instala Docker la primera vez; 2) baja el bundle de código desde GCS;
# 3) levanta los perfiles pedidos en la metadata `perfiles` (streaming, airflow o ambos).
set -euo pipefail
exec > >(tee -a /var/log/lumina-ops.log) 2>&1
echo "== arranque $(date -u +%FT%TZ)"

md() { curl -sf -H 'Metadata-Flavor: Google' \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
PROYECTO="$(md project-id)"
BUCKET_CURATED="$(md bucket-curated)"
BUCKET_RAW="$(md bucket-raw || echo "$PROYECTO-lumina-raw")"
REGION="$(md region || echo us-central1)"
SA_SPARK="$(md sa-spark || echo "lumina-spark@$PROYECTO.iam.gserviceaccount.com")"
PERFILES="$(md perfiles || echo streaming)"
REFRESCO_HORAS="$(md refresco-horas || echo 24)"   # sincronización 2; en demos, minutos

if ! command -v docker >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

mkdir -p /opt/lumina
gcloud storage cp "gs://$BUCKET_CURATED/_ops/bundle.tgz" /tmp/bundle.tgz --quiet
tar -xzf /tmp/bundle.tgz -C /opt/lumina

# Contraseña del admin de Airflow: aleatoria, generada una vez y guardada solo en la VM.
CLAVE=/opt/lumina/.airflow_admin
[[ -s "$CLAVE" ]] || { openssl rand -base64 18 | tr -d '/+=' > "$CLAVE"; chmod 600 "$CLAVE"; }

cat > /opt/lumina/infra/ops/.env <<EOF
COMPOSE_PROFILES=$PERFILES
PROYECTO=$PROYECTO
REGION=$REGION
BUCKET_RAW=$BUCKET_RAW
BUCKET_CURATED=$BUCKET_CURATED
SA_SPARK=$SA_SPARK
REFRESCO_HORAS=$REFRESCO_HORAS
AIRFLOW_ADMIN_PASSWORD=$(cat "$CLAVE")
EOF
chmod 600 /opt/lumina/infra/ops/.env

cd /opt/lumina/infra/ops
echo "== perfiles: $PERFILES"
docker compose up -d --build --remove-orphans
if [[ ",$PERFILES," == *",streaming,"* ]]; then
  COMPOSE_PROFILES="$PERFILES,replay" docker compose build replay
fi
echo "== stack arriba $(date -u +%FT%TZ)"
docker compose ps
