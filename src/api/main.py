"""API de Lumina (capa de exposición): lee BigQuery y sirve JSON al Front-End y a la app.

Corre en Cloud Run (escala a cero). Solo lectura; consultas parametrizadas y con tope de bytes
facturados por consulta, para que una petición mal formada no genere costos.
En producción, las rutas /hogares exigirían el token del cliente (OAuth): aquí los identificadores
vienen seudonimizados del dataset público y la API es de demostración.

Local:  PROYECTO=<id> uvicorn main:app --reload        Documentación interactiva: /docs
"""
from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse
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
PAGINAS = Path(__file__).resolve().parent / "static"


@app.middleware("http")
async def encabezados_cache(request: Request, call_next):
    """El navegador reutiliza las respuestas: 30 s lo que viene del streaming, 1 h la historia.
    Las páginas no se guardan, para que un despliegue nuevo se vea de inmediato."""
    respuesta = await call_next(request)
    ruta = request.url.path
    if request.method != "GET" or respuesta.status_code != 200 or ruta.startswith(("/docs", "/openapi", "/redoc")):
        return respuesta
    if ruta in ("/", "/app", "/consola"):
        respuesta.headers["Cache-Control"] = "no-cache"
    elif ruta.startswith(("/alertas", "/red/", "/zonas/")) or ruta.endswith("/avisos"):
        respuesta.headers["Cache-Control"] = "public, max-age=30"
    else:
        respuesta.headers["Cache-Control"] = "public, max-age=3600"
    return respuesta


# ---------------------------------------------------------------- las dos aplicaciones web
# Mismo origen que la API: las páginas piden sus datos a las rutas de abajo, sin CORS.

@app.get("/", include_in_schema=False)
def inicio():
    return RedirectResponse("/app")


@app.get("/app", include_in_schema=False)
def app_cliente():
    """App del cliente: precios de mañana, cuánto ahorrar, consumo, factura y avisos de su hogar."""
    return FileResponse(PAGINAS / "cliente.html", media_type="text/html; charset=utf-8")


@app.get("/consola", include_in_schema=False)
def consola():
    """Consola interna de Lumina: una vista por rol (control, compras, mantenimiento, planificación, comercial, gerencia)."""
    return FileResponse(PAGINAS / "consola.html", media_type="text/html; charset=utf-8")


def bq() -> bigquery.Client:
    global _cliente
    if _cliente is None:
        _cliente = bigquery.Client(project=PROYECTO)
    return _cliente


# Caché en memoria: casi todo es historia que no cambia, y Cloud Run corre una sola instancia.
# Lo que lee la capa de velocidad (alertas y carga del día) vence a los 30 s para ver lo nuevo del streaming.
_cache: dict[tuple, tuple[float, list[dict[str, Any]]]] = {}
VIVO = ("v_alertas", "v_carga_zona_30min")


def consultar(sql: str, **params: Any) -> list[dict[str, Any]]:
    clave = (sql, tuple(sorted((k, str(v)) for k, v in params.items())))
    guardado = _cache.get(clave)
    if guardado and guardado[0] > time.time():
        return guardado[1]
    filas = _consultar_bq(sql, **params)
    if len(_cache) >= 500:
        _cache.pop(next(iter(_cache)))
    _cache[clave] = (time.time() + (30 if any(v in sql for v in VIVO) else 3600), filas)
    return filas


def _consultar_bq(sql: str, **params: Any) -> list[dict[str, Any]]:
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
    f = dict(filas[0])   # copia: la fila original vive en el caché
    f["ahorro_dtou_gbp"] = round(f["costo_plano_gbp"] - f["costo_dtou_gbp"], 2)
    return f


@app.get("/alertas")
def alertas(tipo: str | None = Query(None, examples=["consumo_anomalo"]), zona: int | None = None,
            desde: date | None = Query(None, examples=["2013-01-14"]), hasta: date | None = Query(None, examples=["2013-01-20"]),
            limite: int = Query(50, ge=1, le=500)):
    """Últimas alertas del streaming: consumo_anomalo, medidor_silencioso, pico_zona, lectura_negativa.
    Con `desde` y `hasta`, solo las de esos días."""
    return consultar(f"""
        SELECT ts_evento, tipo, lclid, zona, transformador, valor, umbral, latencia_s
        FROM {OLAP}.v_alertas`
        WHERE (@tipo = '' OR tipo = @tipo) AND (@zona < 0 OR zona = @zona)
          AND DATE(ts_evento) BETWEEN @desde AND @hasta
        ORDER BY ts_evento DESC LIMIT @limite""",
        tipo=tipo or "", zona=-1 if zona is None else zona,
        desde=desde or date(1900, 1, 1), hasta=hasta or date(2100, 1, 1), limite=limite)


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


# ---------------------------------------------------------------- rutas agregadas para la app web
# Devuelven lo que una pantalla necesita en una sola consulta (todas las zonas o un rango de fechas).

@app.get("/red/carga")
def carga_red(fecha: date = Query(..., examples=["2013-01-16"])):
    """Carga de las 8 zonas cada 30 min en un día, con el umbral p95 y la marca de pico."""
    return uno_o_404(consultar(f"""
        SELECT zona, ts_utc, hora_local, ROUND(kw_por_medidor, 4) AS kw_por_medidor, ROUND(kw_p95, 4) AS kw_p95,
               es_pico, n_medidores, fuente
        FROM {OLAP}.v_carga_zona_30min`
        WHERE fecha = @fecha ORDER BY zona, ts_utc""", fecha=fecha), f"carga del {fecha}")


@app.get("/plan/pronostico")
def pronostico_red(desde: date = Query(..., examples=["2014-01-13"]),
                   hasta: date = Query(..., examples=["2014-01-19"])):
    """Pronóstico D+1 de las 8 zonas frente a lo real y al método ingenuo (máximo 14 días)."""
    if (hasta - desde).days > 14 or hasta < desde:
        raise HTTPException(status_code=400, detail="El rango debe ir de 1 a 14 días.")
    return uno_o_404(consultar(f"""
        SELECT zona, ts_utc, fecha, periodo_local, ROUND(real_kw_por_medidor, 4) AS real,
               ROUND(pronostico_kw_por_medidor, 4) AS pronostico, ROUND(ingenuo_ayer, 4) AS ingenuo
        FROM {ML}.pronostico_zona_d1`
        WHERE fecha BETWEEN @desde AND @hasta ORDER BY zona, ts_utc""", desde=desde, hasta=hasta), "pronóstico")


@app.get("/indicadores")
def indicadores():
    """Métricas fuera de muestra del pronóstico y del mantenimiento predictivo."""
    return {
        "pronostico": consultar(f"SELECT * FROM {ML}.metricas_pronostico` ORDER BY modelo, metodo"),
        "mantenimiento": consultar(f"SELECT * FROM {ML}.metricas_mantenimiento`"),
    }


@app.get("/tarifas/respuesta")
def respuesta_tarifa():
    """Consumo durante los eventos de precio Alto y Bajo frente al consumo normal, por tipo de tarifa (2013)."""
    return consultar(f"SELECT * FROM {ML}.respuesta_eventos_2013` ORDER BY nivel, tarifa")


@app.get("/calidad/resumen")
def calidad_resumen():
    """Medidores por zona y diagnóstico de calidad."""
    return consultar(f"""
        SELECT zona, diagnostico, COUNT(*) AS medidores, ROUND(AVG(cobertura_pct), 1) AS cobertura_media
        FROM {OLAP}.v_calidad_medidores`
        WHERE zona IS NOT NULL GROUP BY zona, diagnostico ORDER BY zona, diagnostico""")


# ---------------------------------------------------------------- rutas de la app del cliente
# Un solo hogar por consulta. En producción, el identificador sale del token del inicio de sesión de Lumina.

@app.get("/hogares/{lclid}/manana")
def manana_hogar(lclid: str, fecha: date = Query(..., examples=["2013-01-19"])):
    """El día siguiente del hogar: consumo total pronosticado por el modelo, repartido en medias horas
    según el perfil del hogar, con el nivel y el precio de la tarifa dinámica de cada media hora."""
    return uno_o_404(consultar(f"""
        WITH pronostico AS (
          SELECT predicted_y AS kwh_dia
          FROM ML.PREDICT(MODEL {ML}.modelo_hogar_d1`,
                          (SELECT * FROM {ML}.features_hogar_diario` WHERE lclid = @lclid AND fecha = @fecha))
        ),
        precios AS (
          SELECT ts_utc, nivel, precio_p_kwh, precio_plano_p_kwh,
                 EXTRACT(HOUR FROM DATETIME(ts_utc, 'Europe/London')) * 2
                   + DIV(EXTRACT(MINUTE FROM DATETIME(ts_utc, 'Europe/London')), 30) + 1 AS periodo_local
          FROM {CUR}.dim_tarifa_dtou` WHERE fecha = @fecha
        ),
        perfil AS (
          SELECT p.periodo_local, p.kwh_media
          FROM {CUR}.perfil_hogar` p
          JOIN {CUR}.dim_fecha` f ON f.fecha = @fecha AND p.tipo_dia = f.tipo_dia
          WHERE p.lclid = @lclid
        )
        SELECT c.ts_utc, c.periodo_local, c.nivel, c.precio_p_kwh, c.precio_plano_p_kwh,
               ROUND(SAFE_DIVIDE(p.kwh_media, SUM(p.kwh_media) OVER ()) * (SELECT kwh_dia FROM pronostico), 4) AS kwh_esperado,
               ROUND((SELECT kwh_dia FROM pronostico), 2) AS kwh_dia_pronostico
        FROM precios c LEFT JOIN perfil p USING (periodo_local)
        ORDER BY c.ts_utc""", lclid=lclid, fecha=fecha), f"el día siguiente de {lclid}")


@app.get("/hogares/{lclid}/dia")
def dia_hogar(lclid: str, fecha: date = Query(..., examples=["2013-01-17"])):
    """Consumo real del hogar cada 30 min en un día, con el nivel y el precio de esa media hora."""
    return uno_o_404(consultar(f"""
        SELECT c.ts_utc, c.periodo_local, c.kwh, t.nivel, t.precio_p_kwh, t.precio_plano_p_kwh
        FROM {CUR}.fact_consumo_30min` c
        JOIN {CUR}.dim_tarifa_dtou` t USING (ts_utc)
        WHERE c.lclid = @lclid AND c.fecha = @fecha ORDER BY c.ts_utc""", lclid=lclid, fecha=fecha), f"consumo de {lclid}")


@app.get("/hogares/{lclid}/factura")
def factura_periodo(lclid: str, desde: date = Query(..., examples=["2013-01-01"]),
                    hasta: date = Query(..., examples=["2013-01-18"])):
    """Lo que el hogar pagó en un período con tarifa dinámica y lo que habría pagado con tarifa plana."""
    if (hasta - desde).days > 366 or hasta < desde:
        raise HTTPException(status_code=400, detail="El rango debe ir de 1 a 366 días.")
    return uno_o_404(consultar(f"""
        SELECT ROUND(SUM(c.kwh), 2) AS kwh,
               ROUND(SUM(c.kwh * t.precio_p_kwh) / 100, 2) AS costo_dinamica_gbp,
               ROUND(SUM(c.kwh * t.precio_plano_p_kwh) / 100, 2) AS costo_plana_gbp,
               ROUND(SUM(IF(t.nivel = 'High', c.kwh, 0)), 2) AS kwh_alto,
               ROUND(SUM(IF(t.nivel = 'Low', c.kwh, 0)), 2) AS kwh_bajo,
               ROUND(SUM(IF(t.nivel = 'High', c.kwh * t.precio_p_kwh, 0)) / 100, 2) AS costo_en_alto_gbp
        FROM {CUR}.fact_consumo_30min` c
        JOIN {CUR}.dim_tarifa_dtou` t USING (ts_utc)
        WHERE c.lclid = @lclid AND c.fecha BETWEEN @desde AND @hasta AND c.kwh IS NOT NULL
        HAVING COUNT(*) > 0""", lclid=lclid, desde=desde, hasta=hasta), f"factura de {lclid}")


@app.get("/hogares/{lclid}/avisos")
def avisos_hogar(lclid: str, limite: int = Query(20, ge=1, le=100)):
    """Alertas del streaming sobre este hogar: consumo anómalo y medidor sin señal."""
    return consultar(f"""
        SELECT ts_evento, tipo, ROUND(valor, 3) AS valor, ROUND(umbral, 3) AS umbral
        FROM {OLAP}.v_alertas`
        WHERE lclid = @lclid ORDER BY ts_evento DESC LIMIT @limite""", lclid=lclid, limite=limite)


@app.get("/tarifas/resumen")
def resumen_tarifas():
    """Factura media de 2013 por tipo de tarifa: lo pagado con tarifa dinámica y lo que se habría pagado con la plana."""
    return consultar(f"""
        SELECT tarifa, COUNT(*) AS hogares, ROUND(AVG(costo_dtou_gbp), 2) AS factura_dinamica_gbp,
               ROUND(AVG(costo_plano_gbp), 2) AS factura_plana_gbp,
               ROUND((1 - SUM(costo_dtou_gbp) / SUM(costo_plano_gbp)) * 100, 2) AS ahorro_pct,
               ROUND(COUNTIF(costo_dtou_gbp < costo_plano_gbp) / COUNT(*) * 100, 1) AS hogares_que_ahorran_pct
        FROM {ML}.factura_tarifas_2013` GROUP BY tarifa ORDER BY tarifa""")
