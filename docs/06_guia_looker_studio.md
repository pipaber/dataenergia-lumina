# Guía: tablero en Looker Studio

Looker Studio es la herramienta gratuita de Google para tableros. Se conecta directo a BigQuery y lee las vistas de `lumina_olap` y las tablas de `lumina_ml`, sin copiar datos. Esta guía arma un tablero de cinco páginas en unos 20–30 minutos.

**Costo.** Looker Studio es gratuito. Cada gráfico lanza una consulta a BigQuery que lee pocos MB, y el primer TB de consultas de cada mes es gratis. Para que los gráficos no repitan consultas, en *Recurso → Administrar fuentes de datos → Frescura de los datos* conviene elegir **12 horas**.

## 1 · Conectar las fuentes de datos

1. Entra a <https://lookerstudio.google.com> con la cuenta de Google del proyecto y elige **Crear → Informe**.
2. En *Agregar datos al informe*, elige el conector **BigQuery** y luego **Mis proyectos → `tespipaber`**.
3. Agrega estas fuentes, una por una (*Recurso → Administrar fuentes de datos → Agregar una fuente de datos*):

| Fuente | Dataset y tabla | Para qué página |
|---|---|---|
| Carga por zona | `lumina_olap.v_carga_zona_30min` | Operación de la red |
| Consumo por segmento | `lumina_olap.v_consumo_segmento_diario` | Tarifa dinámica |
| Factura 2013 | `lumina_ml.factura_tarifas_2013` | Tarifa dinámica |
| Pronóstico por zona | `lumina_ml.pronostico_zona_d1` | Pronóstico D+1 |
| Transformadores | `lumina_ml.ranking_transformadores` | Mantenimiento |
| Medidores en riesgo | `lumina_ml.ranking_medidores` | Mantenimiento |
| Calidad de medidores | `lumina_olap.v_calidad_medidores` | Mantenimiento |
| Alertas | `lumina_olap.v_alertas` | Alertas en tiempo real |

4. En la fuente **Carga por zona**, cambia el tipo del campo `zona` a **Texto**, para que los gráficos la traten como categoría y no como número. Haz lo mismo en *Pronóstico por zona* y *Alertas*.

## 2 · Página «Operación de la red»

Pregunta que responde: ¿cuándo y dónde se carga la red?

| Elemento | Tipo de gráfico | Configuración |
|---|---|---|
| Control de fechas | Control de período | Valor por defecto: 14 a 20 de enero de 2013 (la ola de frío) |
| Control de zona | Lista desplegable | Campo `zona` |
| Carga por zona | Serie temporal | Dimensión `ts_utc`; dimensión de desglose `zona`; métrica `kw_por_medidor` (promedio) |
| Umbral de pico | Serie temporal (misma) | Agrega la métrica `kw_p95` (promedio) como línea punteada |
| Picos por zona | Barras | Dimensión `zona`; métrica: campo calculado `medias_horas_pico` = `SUM(CASE WHEN es_pico THEN 1 ELSE 0 END)` |
| Origen del dato | Anillo | Dimensión `fuente` (batch o streaming); métrica *Recuento de registros* |

El anillo muestra la regla «manda el batch»: con el replay de 2013 todo es `batch`; en producción, lo de hoy aparece como `streaming`.

## 3 · Página «Tarifa dinámica»

Pregunta que responde: ¿la tarifa dinámica ahorra y cambia hábitos?

1. En la fuente *Factura 2013*, crea dos campos calculados:
   - `ahorro_gbp` = `costo_plano_gbp - costo_dtou_gbp`
   - `ahorra` = `CASE WHEN ahorro_gbp > 0 THEN 1 ELSE 0 END`
2. Agrega estos gráficos:

| Elemento | Tipo de gráfico | Configuración |
|---|---|---|
| Factura media | Tabla | Dimensión `tarifa`; métricas `costo_dtou_gbp` y `costo_plano_gbp` (promedio) |
| Hogares que ahorran | Cuadro de resultados | Métrica `ahorra` (promedio), formato de porcentaje; filtro `tarifa = ToU`. Debe dar ≈ 86 % |
| Ahorro por segmento | Barras | Dimensión `acorn_grupo`; desglose `tarifa`; métrica `ahorro_gbp` (promedio) |
| Consumo en punta | Barras (fuente *Consumo por segmento*) | Dimensión `acorn_grupo`; desglose `tarifa`; métrica `pct_en_punta` (promedio) |

## 4 · Página «Pronóstico D+1»

Pregunta que responde: ¿qué tan bien se pronostica la carga de mañana?

| Elemento | Tipo de gráfico | Configuración |
|---|---|---|
| Control de zona | Lista desplegable | Campo `zona`; valor por defecto 3 |
| Control de fechas | Control de período | 13 a 19 de enero de 2014 |
| Real frente a pronóstico | Serie temporal | Dimensión `ts_utc`; métricas `real_kw_por_medidor`, `pronostico_kw_por_medidor` e `ingenuo_ayer` (promedio) |
| Error del modelo | Cuadro de resultados | Campo calculado `wape_modelo` = `SUM(ABS(pronostico_kw_por_medidor - real_kw_por_medidor)) / SUM(real_kw_por_medidor)`, en porcentaje. Para todo ene-feb 2014 debe dar ≈ 4.3 % |
| Error del ingenuo | Cuadro de resultados | `wape_ingenuo` = `SUM(ABS(ingenuo_ayer - real_kw_por_medidor)) / SUM(real_kw_por_medidor)`. Debe dar ≈ 6.8 % |

## 5 · Página «Mantenimiento»

Pregunta que responde: ¿qué revisar primero?

| Elemento | Tipo de gráfico | Configuración |
|---|---|---|
| Transformadores críticos | Tabla (fuente *Transformadores*) | Columnas `ranking`, `zona`, `transformador`, `dias_sobre_12_13`, `dias_sobre_13_14`, `crecimiento_relativo_pct`, `sobrecarga_recurrente`; orden por `ranking`; 15 filas |
| Medidores a inspeccionar | Tabla (fuente *Medidores en riesgo*) | Columnas `ranking`, `lclid`, `zona`, `transformador`, `prob_falla`, `dias_malos_enero`, `diagnostico_historico`; 20 filas |
| Estado del parque | Anillo (fuente *Calidad de medidores*) | Dimensión `diagnostico`; métrica *Recuento de registros* |
| Medidores con problemas por zona | Barras apiladas | Dimensión `zona`; desglose `diagnostico`; filtro `diagnostico != ok` |

## 6 · Página «Alertas en tiempo real»

Pregunta que responde: ¿qué detectó el streaming y qué tan rápido?

| Elemento | Tipo de gráfico | Configuración |
|---|---|---|
| Alertas por tipo | Barras | Dimensión `tipo`; métrica *Recuento de registros* |
| Alertas en el tiempo | Serie temporal | Dimensión `ts_evento`; desglose `tipo` |
| Latencia | Cuadro de resultados | Métrica `latencia_s` (mediana) |
| Detalle | Tabla | `ts_evento`, `tipo`, `lclid`, `zona`, `transformador`, `valor`, `umbral` |

Las alertas que hoy están en la tabla son del replay de la ola de frío. Cada vez que se enciende la demo, aparecen las nuevas.

## 7 · Compartir

En **Compartir**, elige *Cualquier persona con el enlace puede ver*. Quien abra el tablero ve los datos con tus credenciales de BigQuery (opción *Credenciales del propietario* de cada fuente), así que no necesita acceso al proyecto. Al terminar el curso, basta con cambiar el acceso a *Restringido*.
