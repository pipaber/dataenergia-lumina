"""API de Lumina (capa de exposición): lee BigQuery y sirve JSON al Front-End y a la app.

Corre en Cloud Run (escala a cero). Solo lectura; consultas parametrizadas y con tope de bytes
facturados por consulta, para que una petición mal formada no genere costos.
En producción, las rutas /hogares exigirían el token del cliente (OAuth): aquí los identificadores
vienen seudonimizados del dataset público y la API es de demostración.

Local:  PROYECTO=<id> uvicorn main:app --reload        Documentación interactiva: /docs
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery

PROYECTO = os.environ.get("PROYECTO", "tespipaber")
TOPE_BYTES = int(os.environ.get("TOPE_BYTES", 2 * 1024**3))     # 2 GiB por consulta
CUR, OLAP, ML = (f"`{PROYECTO}.{ds}" for ds in ("lumina_curated", "lumina_olap", "lumina_ml"))

app = FastAPI(
    title="API Lumina Energía",
    description="Consumo, carga por zona, pronóstico D+1, alertas, tarifas y mantenimiento predictivo. Datos: Smart Meters in London "
                "(2011-2014), seudonimizados. Proyecto del curso de Big Data, UTEC.",
    version="1.0.0",
)
_cliente: bigquery.Client | None = None


def bq() -> bigquery.Client:
    global _cliente
    if _cliente is None:
        _cliente = bigquery.Client(project=PROYECTO)
    return _cliente


def consultar(sql: str, **params: Any) -> list[dict[str, Any]]:
    tipos = {str: "STRING", int: "INT64", float: "FLOAT64", date: "DATE"}
    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=TOPE_BYTES,
        query_parameters=[bigquery.ScalarQueryParameter(k, tipos[type(v)], v) for k, v in params.items()],
    )
    filas = bq().query(sql, job_config=config, location="US").result()
    return [dict(f.items()) for f in filas]


def uno_o_404(filas: list[dict[str, Any]], que: str) -> list[dict[str, Any]]:
    if not filas:
        raise HTTPException(status_code=404, detail=f"No hay datos de {que}.")
    return filas


@app.get("/salud")
def salud() -> dict[str, str]:
    return {"estado": "ok", "proyecto": PROYECTO}


@app.get("/zonas/{zona}/carga")
def carga_zona(zona: int, fecha: date = Query(..., examples=["2013-01-16"])):
    """Carga de la zona cada 30 min. `fuente` dice si el dato es definitivo (batch) o provisional (streaming)."""
    return uno_o_404(consultar(f"""
        SELECT ts_utc, hora_local, kw_por_medidor, kw_p95, es_pico, n_medidores, fuente
        FROM {OLAP}.v_carga_zona_30min`
        WHERE zona = @zona AND fecha = @fecha ORDER BY ts_utc""", zona=zona, fecha=fecha), f"la zona {zona} el {fecha}")


@app.get("/zonas/{zona}/pronostico")
def pronostico_zona(zona: int, fecha: date = Query(..., examples=["2014-01-15"])):
    """Pronóstico D+1 de la zona (kW por medidor) junto al valor real. Disponible para ene-feb 2014."""
    return uno_o_404(consultar(f"""
        SELECT ts_utc, periodo_local, pronostico_kw_por_medidor, real_kw_por_medidor
        FROM {ML}.pronostico_zona_d1`
        WHERE zona = @zona AND fecha = @fecha ORDER BY ts_utc""", zona=zona, fecha=fecha), f"pronóstico de la zona {zona}")


@app.get("/hogares/{lclid}/consumo")
def consumo_hogar(lclid: str, desde: date = Query(..., examples=["2013-01-14"]),
                  hasta: date = Query(..., examples=["2013-01-20"])):
    """Consumo diario de un hogar (máximo 92 días por consulta)."""
    if (hasta - desde).days > 92 or hasta < desde:
        raise HTTPException(status_code=400, detail="El rango debe ir de 1 a 92 días.")
    return uno_o_404(consultar(f"""
        SELECT fecha, kwh_total, kwh_punta, kwh_max, n_lecturas, cobertura
        FROM {CUR}.fact_consumo_diario`
        WHERE lclid = @lclid AND fecha BETWEEN @desde AND @hasta ORDER BY fecha""",
        lclid=lclid, desde=desde, hasta=hasta), f"consumo de {lclid}")


@app.get("/hogares/{lclid}/pronostico")
def pronostico_hogar(lclid: str):
    """Pronóstico D+1 de kWh del hogar frente a lo real (ene-feb 2014)."""
    return uno_o_404(consultar(f"""
        SELECT fecha, pronostico_kwh, real_kwh
        FROM {ML}.pronostico_hogar_d1`
        WHERE lclid = @lclid ORDER BY fecha""", lclid=lclid), f"pronóstico de {lclid}")


@app.get("/hogares/{lclid}/factura-2013")
def factura_hogar(lclid: str):
    """Factura 2013 del hogar con tarifa dinámica (dToU) y con tarifa plana, para el mismo consumo."""
    filas = uno_o_404(consultar(f"""
        SELECT tarifa, acorn_grupo, kwh_2013, costo_dtou_gbp, costo_plano_gbp, pct_kwh_en_high, pct_kwh_en_low
        FROM {ML}.factura_tarifas_2013` WHERE lclid = @lclid""", lclid=lclid), f"factura de {lclid}")
    f = filas[0]
    f["ahorro_dtou_gbp"] = round(f["costo_plano_gbp"] - f["costo_dtou_gbp"], 2)
    return f


@app.get("/alertas")
def alertas(tipo: str | None = Query(None, examples=["consumo_anomalo"]), zona: int | None = None,
            limite: int = Query(50, ge=1, le=500)):
    """Últimas alertas del streaming: consumo_anomalo, medidor_silencioso, pico_zona, lectura_negativa."""
    return consultar(f"""
        SELECT ts_evento, tipo, lclid, zona, transformador, valor, umbral, latencia_s
        FROM {OLAP}.v_alertas`
        WHERE (@tipo = '' OR tipo = @tipo) AND (@zona < 0 OR zona = @zona)
        ORDER BY ts_evento DESC LIMIT @limite""",
        tipo=tipo or "", zona=-1 if zona is None else zona, limite=limite)


@app.get("/mantenimiento/medidores")
def medidores_en_riesgo(zona: int | None = None, limite: int = Query(20, ge=1, le=200)):
    """Medidores con mayor probabilidad de fallar el mes siguiente (modelo de BigQuery ML)."""
    return consultar(f"""
        SELECT ranking, lclid, zona, transformador, prob_falla, fallo_en_enero, dias_malos_enero,
               dias_cero_enero, dias_malos_3m, diagnostico_historico
        FROM {ML}.ranking_medidores`
        WHERE (@zona < 0 OR zona = @zona)
        ORDER BY ranking LIMIT @limite""", zona=-1 if zona is None else zona, limite=limite)


@app.get("/mantenimiento/transformadores")
def transformadores_sobrecargados(limite: int = Query(20, ge=1, le=112)):
    """Transformadores con más días sobre la capacidad de referencia en hora punta, en dos inviernos."""
    return consultar(f"""
        SELECT ranking, zona, transformador, dias_sobre_12_13, dias_sobre_13_14, sobrecarga_recurrente,
               crecimiento_relativo_pct, max_kw_por_medidor_13_14, capacidad_ref_kw_por_medidor
        FROM {ML}.ranking_transformadores`
        ORDER BY ranking LIMIT @limite""", limite=limite)


@app.get("/segmentos/consumo")
def consumo_segmentos(desde: date = Query(..., examples=["2013-01-01"]),
                      hasta: date = Query(..., examples=["2013-01-31"])):
    """Consumo medio por hogar según segmento Acorn y tarifa (solo grupos de 10 hogares o más)."""
    return uno_o_404(consultar(f"""
        SELECT acorn_grupo, tarifa, SUM(hogares) AS hogares_dia, ROUND(AVG(kwh_medio_hogar), 3) AS kwh_medio_hogar,
               ROUND(AVG(pct_en_punta), 1) AS pct_en_punta
        FROM {OLAP}.v_consumo_segmento_diario`
        WHERE fecha BETWEEN @desde AND @hasta
        GROUP BY acorn_grupo, tarifa ORDER BY acorn_grupo, tarifa""", desde=desde, hasta=hasta), "segmentos")
