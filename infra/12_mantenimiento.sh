#!/usr/bin/env bash
# Fase 4 · Mantenimiento predictivo: riesgo de falla por medidor (BigQuery ML) y transformadores con
# sobrecarga recurrente. Costo medido: ~US$0.18 (el modelo factura ~30 GiB al iterar sobre las variables).
# Al final imprime las métricas fuera de muestra, los rankings y el costo real.
source "$(dirname "$0")/_common.sh"
require_config
BQ=(bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql)
T="\`$PROJECT_ID.lumina_ml"
INICIO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

log "Entrenando el modelo de fallas y calculando los rankings"
sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/mantenimiento.sql" | "${BQ[@]}" >/dev/null

log "Métricas fuera de muestra (jul-dic 2013): inspeccionar el 5 % de mayor riesgo cada mes"
"${BQ[@]}" --format pretty "SELECT * FROM $T.metricas_mantenimiento\`"
"${BQ[@]}" --format pretty "SELECT ROUND(roc_auc, 3) AS auc FROM ML.EVALUATE(MODEL $T.modelo_falla_medidor\`,
  (SELECT * FROM $T.features_medidor_mes\` WHERE conjunto = 'prueba'))"

log "Medidores con mayor riesgo de falla en feb-2014 (top 10)"
"${BQ[@]}" --format pretty "SELECT * FROM $T.ranking_medidores\` WHERE ranking <= 10 ORDER BY ranking"

log "Transformadores con sobrecarga en hora punta (top 10)"
"${BQ[@]}" --format pretty "SELECT * FROM $T.ranking_transformadores\` WHERE ranking <= 10 ORDER BY ranking"
"${BQ[@]}" --format pretty "SELECT COUNTIF(sobrecarga_recurrente) AS recurrentes, COUNT(*) AS transformadores,
  ANY_VALUE(capacidad_ref_kw_por_medidor) AS capacidad_ref FROM $T.ranking_transformadores\`"

log "Costo real de esta corrida"
"${BQ[@]}" --format pretty "
SELECT statement_type, COUNT(*) AS trabajos, ROUND(SUM(total_bytes_billed) / POW(1024, 3), 3) AS gib_facturados,
       ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 4) AS usd_aprox
FROM \`region-us\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP('$INICIO') AND project_id = '$PROJECT_ID'
  AND statement_type != 'SCRIPT'   -- el SCRIPT padre repite los bytes de sus hijos
GROUP BY statement_type ORDER BY gib_facturados DESC"
