-- Fase 4 · Mantenimiento predictivo y planificación (vertical IV).
-- 1. Medidores: un BOOSTED_TREE_CLASSIFIER predice qué medidor fallará el mes siguiente a partir
--    de las señales del mes actual y de los 3 anteriores. Falla = al menos 2 días sin datos o con
--    cobertura < 90 %, o al menos 2 días en cero (incluye al medidor que deja de reportar).
--    Entrenamiento: meses hasta jun-2013; prueba: jul a dic-2013; ranking actual: ene-2014.
-- 2. Transformadores: sobrecarga recurrente en hora punta (17 a 20 h local) en los dos inviernos,
--    contra una capacidad de referencia supuesta: el percentil 99 de la carga por medidor en hora
--    punta del invierno 2012-13 (el dataset no trae la capacidad nominal de cada transformador).
-- Uso: bash infra/12_mantenimiento.sh      ${P} = proyecto

-- ------------------------------------------------------------------ medidores · variables por mes
CREATE OR REPLACE TABLE `${P}.lumina_ml.features_medidor_mes` AS
WITH dia AS (
  SELECT lclid, fecha, kwh_total, cobertura
  FROM `${P}.lumina_curated.fact_consumo_diario`
),
vida AS (
  SELECT lclid, DATE_TRUNC(MIN(fecha), MONTH) AS primer_mes FROM dia GROUP BY lclid
),
calendario AS (   -- todos los meses desde el primer mes completo, haya o no datos
  SELECT v.lclid, mes
  FROM vida v, UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(v.primer_mes, INTERVAL 1 MONTH), '2014-01-01', INTERVAL 1 MONTH)) AS mes
),
por_mes AS (
  SELECT lclid, DATE_TRUNC(fecha, MONTH) AS mes, COUNT(*) AS dias_con_dato,
         COUNTIF(cobertura < 0.9) AS dias_baja, COUNTIF(kwh_total = 0) AS dias_cero, AVG(kwh_total) AS kwh_medio
  FROM dia GROUP BY 1, 2
),
base AS (
  SELECT c.lclid, c.mes,
         EXTRACT(DAY FROM LAST_DAY(c.mes)) - IFNULL(p.dias_con_dato, 0) AS dias_sin_dato,
         IFNULL(p.dias_baja, 0) AS dias_baja, IFNULL(p.dias_cero, 0) AS dias_cero, p.kwh_medio
  FROM calendario c LEFT JOIN por_mes p USING (lclid, mes)
),
marcado AS (
  SELECT *, (dias_sin_dato + dias_baja >= 2 OR dias_cero >= 2) AS falla FROM base
),
historia AS (
  SELECT
    *,
    LEAD(falla) OVER w AS falla_mes_siguiente,
    IFNULL(SUM(CAST(falla AS INT64)) OVER w3, 0) AS fallas_3m,
    IFNULL(SUM(dias_sin_dato + dias_baja) OVER w3, 0) AS dias_malos_3m,
    IFNULL(SUM(dias_cero) OVER w3, 0) AS dias_cero_3m,
    AVG(kwh_medio) OVER w3 AS kwh_medio_3m,
    ROW_NUMBER() OVER w AS meses_activo
  FROM marcado
  WINDOW w AS (PARTITION BY lclid ORDER BY mes),
         w3 AS (PARTITION BY lclid ORDER BY mes ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
  hi.lclid, hi.mes, hi.falla, hi.falla_mes_siguiente, hi.dias_sin_dato, hi.dias_baja, hi.dias_cero,
  ROUND(hi.kwh_medio, 3) AS kwh_medio, hi.fallas_3m, hi.dias_malos_3m, hi.dias_cero_3m,
  ROUND(SAFE_DIVIDE(hi.kwh_medio, hi.kwh_medio_3m), 3) AS ratio_consumo, hi.meses_activo,
  EXTRACT(MONTH FROM hi.mes) AS mes_anio, h.acorn_grupo, h.tarifa, h.zona, h.transformador,
  CASE WHEN hi.mes < '2013-07-01' THEN 'entrenamiento'
       WHEN hi.mes < '2014-01-01' THEN 'prueba'
       ELSE 'actual' END AS conjunto
FROM historia hi
JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
WHERE hi.kwh_medio IS NOT NULL                                -- el medidor reportó algo ese mes
  AND (hi.falla_mes_siguiente IS NOT NULL OR hi.mes = '2014-01-01');

CREATE OR REPLACE MODEL `${P}.lumina_ml.modelo_falla_medidor`
OPTIONS (model_type = 'BOOSTED_TREE_CLASSIFIER', input_label_cols = ['falla_mes_siguiente'],
         auto_class_weights = TRUE, max_iterations = 50, early_stop = TRUE,
         data_split_method = 'RANDOM', data_split_eval_fraction = 0.1) AS
SELECT falla_mes_siguiente, falla, dias_sin_dato, dias_baja, dias_cero, kwh_medio, fallas_3m,
       dias_malos_3m, dias_cero_3m, ratio_consumo, meses_activo, mes_anio, acorn_grupo, tarifa
FROM `${P}.lumina_ml.features_medidor_mes`
WHERE conjunto = 'entrenamiento';

CREATE OR REPLACE TABLE `${P}.lumina_ml.prediccion_falla_medidor` AS
SELECT p.lclid, p.mes, p.conjunto, p.zona, p.transformador, p.falla, p.falla_mes_siguiente,
       p.dias_malos_3m, p.dias_cero_3m, p.dias_sin_dato, p.dias_baja, p.dias_cero,
       ROUND((SELECT prob FROM UNNEST(p.predicted_falla_mes_siguiente_probs) WHERE label), 4) AS prob_falla
FROM ML.PREDICT(MODEL `${P}.lumina_ml.modelo_falla_medidor`,
                (SELECT * FROM `${P}.lumina_ml.features_medidor_mes` WHERE conjunto IN ('prueba', 'actual'))) p;

-- Métricas fuera de muestra (jul a dic-2013). Dos presupuestos de inspección por mes:
--   a) el 5 % de los medidores con mayor riesgo;
--   b) tantos medidores como fallaron este mes, para comparar con la regla ingenua con el mismo esfuerzo.
CREATE OR REPLACE TABLE `${P}.lumina_ml.metricas_mantenimiento` AS
WITH r AS (
  SELECT *,
         PERCENT_RANK() OVER (PARTITION BY mes ORDER BY prob_falla DESC) AS pr,
         ROW_NUMBER() OVER (PARTITION BY mes ORDER BY prob_falla DESC, dias_malos_3m DESC, dias_cero_3m DESC) AS puesto,
         COUNTIF(falla) OVER (PARTITION BY mes) AS fallaron_este_mes
  FROM `${P}.lumina_ml.prediccion_falla_medidor` WHERE conjunto = 'prueba'
),
total AS (SELECT COUNTIF(falla_mes_siguiente) AS fallas FROM r)
SELECT 'modelo: 5 % con mayor riesgo' AS metodo, COUNTIF(pr < 0.05) AS inspeccionados,
       COUNTIF(pr < 0.05 AND falla_mes_siguiente) AS fallas_encontradas,
       ROUND(SAFE_DIVIDE(COUNTIF(pr < 0.05 AND falla_mes_siguiente), COUNTIF(pr < 0.05)) * 100, 1) AS precision_pct,
       ROUND(SAFE_DIVIDE(COUNTIF(pr < 0.05 AND falla_mes_siguiente), ANY_VALUE(t.fallas)) * 100, 1) AS cobertura_pct
FROM r CROSS JOIN total t
UNION ALL
SELECT 'modelo: mismo número que el ingenuo', COUNTIF(puesto <= fallaron_este_mes),
       COUNTIF(puesto <= fallaron_este_mes AND falla_mes_siguiente),
       ROUND(SAFE_DIVIDE(COUNTIF(puesto <= fallaron_este_mes AND falla_mes_siguiente), COUNTIF(puesto <= fallaron_este_mes)) * 100, 1),
       ROUND(SAFE_DIVIDE(COUNTIF(puesto <= fallaron_este_mes AND falla_mes_siguiente), ANY_VALUE(t.fallas)) * 100, 1)
FROM r CROSS JOIN total t
UNION ALL
SELECT 'ingenuo: los que fallaron este mes', COUNTIF(falla), COUNTIF(falla AND falla_mes_siguiente),
       ROUND(SAFE_DIVIDE(COUNTIF(falla AND falla_mes_siguiente), COUNTIF(falla)) * 100, 1),
       ROUND(SAFE_DIVIDE(COUNTIF(falla AND falla_mes_siguiente), ANY_VALUE(t.fallas)) * 100, 1)
FROM r CROSS JOIN total t
UNION ALL
SELECT 'al azar (tasa de falla)', COUNT(*), COUNTIF(falla_mes_siguiente),
       ROUND(COUNTIF(falla_mes_siguiente) / COUNT(*) * 100, 1), 100.0
FROM r;

-- Ranking para la cuadrilla: riesgo de falla en feb-2014 según lo observado hasta ene-2014.
-- Desempate: más días malos y más días en cero en enero.
CREATE OR REPLACE TABLE `${P}.lumina_ml.ranking_medidores` AS
SELECT
  ROW_NUMBER() OVER (ORDER BY p.prob_falla DESC, p.dias_sin_dato + p.dias_baja DESC, p.dias_cero DESC, p.dias_malos_3m DESC) AS ranking,
  p.lclid, p.zona, p.transformador, p.prob_falla, p.falla AS fallo_en_enero,
  p.dias_sin_dato + p.dias_baja AS dias_malos_enero, p.dias_cero AS dias_cero_enero,
  p.dias_malos_3m, p.dias_cero_3m, q.diagnostico AS diagnostico_historico
FROM `${P}.lumina_ml.prediccion_falla_medidor` p
LEFT JOIN `${P}.lumina_curated.dq_medidor` q USING (lclid)
WHERE p.conjunto = 'actual';

-- ------------------------------------------------------------------ transformadores
CREATE OR REPLACE TABLE `${P}.lumina_ml.ranking_transformadores` AS
WITH carga AS (
  SELECT zona, transformador, fecha, kw_medio / n_medidores AS kw_por_medidor,
         CASE WHEN fecha BETWEEN '2012-12-01' AND '2013-02-28' THEN '2012-13'
              WHEN fecha BETWEEN '2013-12-01' AND '2014-02-28' THEN '2013-14' END AS invierno
  FROM `${P}.lumina_curated.fact_carga_transformador_30min`
  WHERE n_medidores >= 20
    AND EXTRACT(HOUR FROM DATETIME(ts_utc, 'Europe/London')) BETWEEN 17 AND 20
),
referencia AS (
  SELECT APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(99)] AS capacidad_ref
  FROM carga WHERE invierno = '2012-13'
),
por_invierno AS (
  SELECT c.zona, c.transformador, c.invierno,
         APPROX_QUANTILES(c.kw_por_medidor, 100)[OFFSET(95)] AS p95,
         MAX(c.kw_por_medidor) AS maximo,
         COUNT(DISTINCT IF(c.kw_por_medidor > r.capacidad_ref, c.fecha, NULL)) AS dias_sobre_capacidad
  FROM carga c CROSS JOIN referencia r
  WHERE c.invierno IS NOT NULL
  GROUP BY 1, 2, 3
),
global AS (
  SELECT invierno, APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(95)] AS p95_global
  FROM carga WHERE invierno IS NOT NULL GROUP BY invierno
),
pareado AS (
  SELECT a.zona, a.transformador,
         a.dias_sobre_capacidad AS dias_sobre_12_13, b.dias_sobre_capacidad AS dias_sobre_13_14,
         ROUND(b.maximo, 3) AS max_kw_por_medidor_13_14,
         -- crecimiento de la punta frente al de toda la red (el invierno 2013-14 fue más templado)
         ROUND((SAFE_DIVIDE(b.p95, a.p95) / SAFE_DIVIDE(g2.p95_global, g1.p95_global) - 1) * 100, 1) AS crecimiento_relativo_pct
  FROM por_invierno a
  JOIN por_invierno b ON b.transformador = a.transformador AND b.invierno = '2013-14'
  JOIN global g1 ON g1.invierno = '2012-13'
  JOIN global g2 ON g2.invierno = '2013-14'
  WHERE a.invierno = '2012-13'
)
SELECT
  RANK() OVER (ORDER BY p.dias_sobre_12_13 + p.dias_sobre_13_14 DESC, p.crecimiento_relativo_pct DESC) AS ranking,
  p.*,
  (p.dias_sobre_12_13 > 0 AND p.dias_sobre_13_14 > 0) AS sobrecarga_recurrente,
  ROUND(r.capacidad_ref, 3) AS capacidad_ref_kw_por_medidor
FROM pareado p CROSS JOIN referencia r;
