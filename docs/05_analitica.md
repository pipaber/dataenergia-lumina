# Analítica: vistas OLAP, tarifas, pronóstico D+1 y mantenimiento

La Fase 4 convierte las tablas de la Fase 2 y las alertas de la Fase 3 en respuestas para el negocio. Todo corre en **BigQuery** desde la laptop, sin clústeres ni VM: son consultas SQL y modelos de BigQuery ML que se pagan por bytes leídos. Las corridas de esta fase leyeron unos 98 GiB en total, **≤ US$0.61**.

| Paso | Script | Qué crea | Vertical |
|---|---|---|---|
| 1 | [`infra/08_tarifas_dtou.sh`](../infra/08_tarifas_dtou.sh) | Calendario oficial de la tarifa dinámica 2013 → `lumina_curated.dim_tarifa_dtou` | III · Informes |
| 2 | [`infra/09_vistas_olap.sh`](../infra/09_vistas_olap.sh) | 5 vistas en `lumina_olap` que unen historia y hoy | III · Informes |
| 3 | [`infra/11_tarifas.sh`](../infra/11_tarifas.sh) | Factura 2013 con ambas tarifas y respuesta a los eventos de precio | III · Informes |
| 4 | [`infra/10_pronostico.sh`](../infra/10_pronostico.sh) | Dos modelos D+1 (zona y hogar), sus pronósticos y métricas | II · Planes diarios |
| 5 | [`infra/12_mantenimiento.sh`](../infra/12_mantenimiento.sh) | Modelo de fallas por medidor y ranking de transformadores con sobrecarga | IV · Mantenimiento |
| 6 | [`infra/13_api_cloud_run.sh`](../infra/13_api_cloud_run.sh) | API REST de solo lectura publicada en Cloud Run | Exposición |

## Vistas que unen historia y hoy (sincronización 3 y 4)

Las vistas son la **capa de servicio** del patrón Lambda: el tablero y la API consultan una sola vista y no necesitan saber si un dato vino del batch o del streaming. El SQL está en [`src/sql/olap_vistas.sql`](../src/sql/olap_vistas.sql).

| Vista | Qué responde | Filas hoy |
|---|---|---|
| `v_carga_zona_30min` | Carga de cada zona cada 30 min, con `fuente` = `batch` (definitivo) o `streaming` (provisional) y `es_pico` | 317,746 |
| `v_consumo_segmento_diario` | Consumo medio por hogar según segmento Acorn y tarifa (solo grupos de 10 hogares o más, cobertura ≥ 90 %) | 6,029 |
| `v_alertas` | Alertas del streaming sin duplicados, con su latencia | 1,939 |
| `v_calidad_medidores` | Huecos, rachas de ceros y lecturas negativas por medidor (base del mantenimiento) | 5,561 |
| `v_red_vs_muestra` | Demanda nacional del Reino Unido frente a la muestra de Londres | 39,727 |

**Regla 4, "manda el batch".** En `v_carga_zona_30min`, de `lumina_rt` solo entran las medias horas **posteriores al último dato del batch**. Hoy esa parte aporta 0 filas, y es lo correcto: el replay de la Fase 3 reprodujo enero de 2013, que el batch ya tiene como dato definitivo. En producción, `streaming` aparece solo entre el último batch nocturno y el momento de la consulta. Dentro de `rt` se conserva la última versión de cada media hora (`ROW_NUMBER` por `ts_procesado`), porque Spark puede reemitir una ventana cuando llegan lecturas atrasadas.

La marca `es_pico` usa la misma regla que el streaming: carga sobre el p95 de la zona para su estación y tipo de día, solo de 17 a 20 h. Así el informe histórico y la alerta en vivo coinciden. Con esa regla hay 3,888 medias horas pico en toda la historia.

## Tarifa dinámica frente a tarifa plana (2013)

En 2013, 1,038 hogares de la muestra tuvieron una **tarifa dinámica** (dToU): el precio de cada media hora era Alto, Normal o Bajo y se avisaba un día antes. Los demás pagaron una tarifa plana. El calendario oficial (UK Power Networks, licencia CC BY 4.0) trae las 17,520 medias horas del año. [`src/ingestion/tarifas_dtou.py`](../src/ingestion/tarifas_dtou.py) lo convierte a CSV en UTC.

| Nivel | Precio (p/kWh) | Uso |
|---|---|---|
| Alto | 67.20 | Eventos de escasez |
| Normal | 11.76 | El resto del año |
| Bajo | 3.99 | Exceso de oferta (por ejemplo, mucho viento) |
| Plana (comparación) | 14.228 | Todo el año |

**Factura con el mismo consumo y dos precios.** [`src/sql/tarifas.sql`](../src/sql/tarifas.sql) toma a cada hogar con al menos el 90 % de las medias horas de 2013 y calcula su factura con ambas tarifas.

| Tarifa del hogar | Hogares | Factura dinámica | Factura plana | Diferencia | Hogares que ahorran |
|---|---|---|---|---|---|
| ToU (recibió señales) | 1,038 | £475.85 | £489.27 | **−2.74 %** | 86.4 % |
| Std (control) | 4,069 | | | −1.48 % | |

**Respuesta a la demanda.** Se compara lo que consumió cada hogar durante un evento con su consumo normal a la misma media hora y en el mismo mes. Los hogares Std nunca recibieron señales: son el grupo de control.

| Evento | ToU | Std (control) | Efecto de la tarifa |
|---|---|---|---|
| Precio Alto | 0.954 (−4.6 %) | 1.010 (+1.0 %) | **−5.6 %** de consumo en la punta |
| Precio Bajo | 1.068 (+6.8 %) | 1.005 (+0.5 %) | +6.3 % hacia las horas baratas |

Los hogares con tarifa dinámica sí responden al precio. El ahorro de la factura no se explica solo por el calendario, porque los hogares Std, con el mismo calendario, ahorran menos (−1.48 %). En la [sustentación de la Entrega 1](entrega1/sustentacion_entrega1.docx) (Valor para el negocio), ese recorte de 5.6 % se traduce en ≈ 270 MW menos de compras de emergencia en la punta.

## Pronóstico del día siguiente (D+1)

El caso pide planes diarios: cuánta energía comprar mañana y dónde. [`src/sql/pronostico.sql`](../src/sql/pronostico.sql) entrena dos modelos `BOOSTED_TREE_REGRESSOR` con BigQuery ML.

| Modelo | Qué pronostica | Series | Filas de prueba |
|---|---|---|---|
| `modelo_zona_d1` | kW por medidor de cada zona, cada 30 min | 8 zonas × 48 medias horas | 22,280 |
| `modelo_hogar_d1` | kWh del día por hogar | 5,561 hogares | 290,034 |

**Variables.** Solo se usa lo que se conoce al cierre del día D: el valor de ayer y de hace 7 días a la misma media hora, el promedio de los últimos 7 días (y de 28 días para el hogar), el calendario (día de la semana, feriado, estación) y el clima de D+1. El clima de D+1 es el **observado**, usado como si fuera un pronóstico perfecto. Es un supuesto declarado: en producción se usaría el pronóstico meteorológico, con algo más de error.

**Validación.** Se entrena con 2012–2013 y se mide fuera de muestra en enero y febrero de 2014, el invierno, cuando más importa acertar. La referencia es el **método ingenuo**: mañana será igual que hoy.

| Modelo | Método | MAPE | WAPE |
|---|---|---|---|
| Zona · media hora | Boosted tree | **4.4 %** | **4.3 %** |
| Zona · media hora | Ingenuo (igual que ayer) | 6.9 % | 6.8 % |
| Hogar · día | Boosted tree | 39.8 % | **18.5 %** |
| Hogar · día | Ingenuo (igual que ayer) | 28.3 % | 21.3 % |

MAPE es el error porcentual medio de cada punto. WAPE es el error total dividido por la energía total, y es el que pesa en una compra de energía.

- **Por zona, el modelo reduce el error 36 %.** Es el pronóstico que alimenta el plan de compras D+1.
- **Por hogar, el modelo gana en WAPE pero pierde en MAPE.** El 4.5 % de los días-hogar consume menos de 2 kWh (casa vacía, viaje), y ahí cualquier error se vuelve un porcentaje enorme. El modelo pronostica un día típico, mientras que el ingenuo repite el día vacío de ayer.

| Días-hogar | Proporción | MAPE modelo | MAPE ingenuo | Mediana del error, modelo | Mediana del error, ingenuo |
|---|---|---|---|---|---|
| Consumo real ≥ 2 kWh | 95.5 % | **19.1 %** | 23.2 % | **13.5 %** | 15.3 % |
| Consumo real < 2 kWh | 4.5 % | 522 % | 149 % | 36.2 % | 6.7 % |

Para la app de clientes basta el pronóstico por hogar tal como está. Una mejora posible es una regla simple: si ayer el hogar consumió casi nada, mañana probablemente también.

**Costo.** Los boosted trees releen los datos en cada iteración, por eso facturan 60 GiB aunque las tablas de variables pesen menos de 1 GiB. Se eligieron por precio: BigQuery ML los cobra a la tarifa de consultas (US$6.25/TiB), mientras que `LINEAR_REG` y `ARIMA_PLUS` cuestan US$312.50/TiB.

## Mantenimiento predictivo

La vertical IV pregunta qué medidores fallan y qué zonas se sobrecargan. [`src/sql/mantenimiento.sql`](../src/sql/mantenimiento.sql) responde con dos rankings, ambos en BigQuery, por ≈ US$0.19.

**Medidores: ¿cuál fallará el mes siguiente?** Un medidor «falla» en un mes si tiene al menos 2 días sin datos o con menos del 90 % de sus lecturas, o al menos 2 días en cero; eso incluye al que deja de reportar. Un modelo `BOOSTED_TREE_CLASSIFIER` usa las señales del mes actual y de los 3 anteriores (días malos, días en cero, cambio de consumo, antigüedad, segmento). Se entrenó con los meses hasta junio de 2013 y se evaluó de julio a diciembre de 2013. Su AUC es **0.82**: ordena bien a los medidores por riesgo.

| Si cada mes se revisa… | Revisados | Fallas encontradas | Acierto | Fallas cubiertas |
|---|---:|---:|---:|---:|
| El 5 % de medidores con mayor riesgo según el modelo | 1,585 | 638 | 40.3 % | **62.1 %** |
| Los que fallaron este mes (regla simple) | 749 | 554 | 74.0 % | 53.9 % |
| El mismo número que la regla simple, según el modelo | 749 | 551 | 73.6 % | 53.7 % |
| Al azar | — | — | 3.3 % | — |

La lectura honesta: **con el mismo presupuesto de inspección, el modelo empata con la regla simple**, porque un medidor que falló tiende a seguir fallando. Su valor está en ir más allá de esa lista: al ampliar la revisión al 5 %, los 836 medidores adicionales tienen un 10 % de acierto, tres veces más que al azar, y se cubre el 62 % de las fallas en lugar del 54 %. El ranking para la cuadrilla está en `lumina_ml.ranking_medidores`.

**Transformadores: ¿cuáles se sobrecargan cada invierno?** El dataset no trae la capacidad nominal de cada transformador, así que se usa una **capacidad de referencia supuesta**: el percentil 99 de la carga por medidor en hora punta del invierno 2012-13 (1.16 kW por medidor). Se cuentan los días en que cada transformador la superó en los dos inviernos y el crecimiento de su punta frente al de toda la red, porque el invierno 2013-14 fue más templado.

| Resultado | Valor |
|---|---|
| Transformadores con sobrecarga en **ambos** inviernos | 14 de 112 |
| El más crítico | Z7-T12: 48 días sobre la capacidad en 2012-13 y 33 en 2013-14, con la punta creciendo 2.4 % más que la red |

En producción, la capacidad de referencia sería la potencia nominal de cada transformador, que tienen las distribuidoras.

## API de consulta

[`src/api/main.py`](../src/api/main.py) es una API FastAPI de solo lectura, pensada para Cloud Run (escala a cero, sin costo cuando nadie la usa). Usa consultas parametrizadas y un tope de 2 GiB facturados por consulta, para que una petición mal formada no genere gasto.

| Ruta | Devuelve |
|---|---|
| `/zonas/{zona}/carga?fecha=` | Carga de la zona cada 30 min, con su fuente (batch o streaming) |
| `/zonas/{zona}/pronostico?fecha=` | Pronóstico D+1 frente a lo real |
| `/hogares/{lclid}/consumo?desde=&hasta=` | Consumo diario (hasta 92 días) |
| `/hogares/{lclid}/pronostico` | Pronóstico D+1 del hogar |
| `/hogares/{lclid}/factura-2013` | Factura con tarifa dinámica y plana, y el ahorro |
| `/alertas?tipo=&zona=` | Últimas alertas del streaming |
| `/segmentos/consumo?desde=&hasta=` | Consumo por segmento Acorn y tarifa |

| `/mantenimiento/medidores?zona=&limite=` | Medidores con mayor riesgo de falla |
| `/mantenimiento/transformadores?limite=` | Transformadores con sobrecarga en hora punta |

**Publicada en <https://lumina-api-csascndwia-uc.a.run.app/docs>** (documentación interactiva). Para la demo es pública, con un máximo de 1 instancia y una cuenta de servicio que solo puede leer los datasets que usa. Las rutas probadas responden en 1–2 s. Se borra con `bash infra/13_api_cloud_run.sh borrar`.

## Las dos aplicaciones

La API sirve también dos aplicaciones web, una por tipo de usuario. Cada una tiene un solo objetivo y usa solo los outputs que su usuario necesita.

| Aplicación | Dirección | Usuario | Objetivo | Outputs que usa |
|---|---|---|---|---|
| **App del cliente** | [https://lumina-api-csascndwia-uc.a.run.app/app](https://lumina-api-csascndwia-uc.a.run.app/app) | El hogar (se descarga en el celular) | Pagar menos usando la luz fuera de las horas de precio Alto | Pronóstico del hogar · factura con tarifa dinámica frente a plana · avisos de consumo anómalo y de medidor sin señal · precios de mañana |
| **Consola interna** | [https://lumina-api-csascndwia-uc.a.run.app/consola](https://lumina-api-csascndwia-uc.a.run.app/consola) | El personal de Lumina, con una vista por rol | Operar, comprar energía, mantener y planificar la red | Picos por zona · alertas de la red · pronóstico por zona · ranking de medidores y transformadores · calidad · respuesta a la tarifa · segmentos · indicadores |

**App del cliente.** Su "hoy" es el viernes 18 de enero de 2013, en plena ola de frío, y tiene cuatro pestañas:

- **Mañana:** los precios de mañana como un horario y cuánto ahorra moviendo el consumo de las horas de precio Alto. El total del día lo pronostica el modelo y se reparte por hora según el perfil del hogar.
- **Consumo:** hoy frente a su promedio, las últimas dos semanas y el día de ayer con el precio de cada media hora.
- **Factura:** lo que lleva pagado en el mes con tarifa dinámica y con plana.
- **Avisos:** consumo inusual, medidor sin señal y aviso de precio.

**Consola interna.** Seis vistas, una por rol: centro de control, compras de energía, mantenimiento, planificación de red, comercial y tarifas, y gerencia. Cada vista abre con la respuesta a la pregunta de ese rol, en una frase, y dice qué período muestra:

- el centro de control ve la reproducción de la ola de frío de enero de 2013;
- compras ve los pronósticos de enero de 2014;
- planificación compara los inviernos 2012-13 y 2013-14.

Las rutas que usan las dos aplicaciones son de solo lectura y tienen el mismo tope de 2 GiB por consulta. En producción, la app del cliente tomaría el hogar del token del inicio de sesión de Lumina, y no de la dirección.

## Pendiente de esta fase

- Ensayar la demo con el [guion](08_guion_demo.md).
- Opcional: armar el tablero en Looker Studio con la [guía](06_guia_looker_studio.md); la consola interna ya cubre ese uso.
