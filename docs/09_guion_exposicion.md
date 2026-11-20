# Guion de la exposición (Entrega 3)

Cómo presentar el proyecto el 21 de noviembre: qué se dice en cada diapositiva, cuánto dura y quién la expone. El PDF del curso dice que habrá nota individual y grupal, así que cada integrante expone un bloque completo y responde las preguntas de su parte.

**Duración sugerida:** unos 25 minutos, 13 de diapositivas y 12 de demo. Ajusten los tiempos a lo que indique la profesora.

**Materiales:**
- Presentación (15 diapositivas, con notas para el expositor en cada una).
- [Guion de la demo](08_guion_demo.md), con los comandos y qué decir minuto a minuto.
- App del cliente: <https://lumina-api-csascndwia-uc.a.run.app/app>
- Consola interna: <https://lumina-api-csascndwia-uc.a.run.app/consola>

## Reparto en cuatro bloques

Los bloques siguen las cuatro secciones de la presentación. Con tres integrantes, uno toma los bloques 1 y 4; con cinco, la demo se divide en dos.

| Bloque | Diapositivas | Tiempo | Quién expone | Idea que debe quedar |
|---|---|---|---|---|
| **1 · El problema y la propuesta** | 1 Portada · 2 Resumen ejecutivo · 3 Situación actual · 4 Criterios de éxito | 4 min | Integrante 1 | La hora punta es cara, las lecturas existen pero solo sirven para facturar, y el prototipo ya cumple los indicadores |
| **2 · La arquitectura** | 5 Productos · 6 Supuestos · 7 Arquitectura · 8 Patrón Lambda · 9 Resiliencia | 5 min | Integrante 2 | Código abierto sobre servicios gestionados; batch y streaming sincronizados; dos regiones y respaldos que nadie puede borrar |
| **3 · Costo y demo** | 10 Calculadora · 11 Dos aplicaciones · 12 Demo en vivo | 2 min + 12 min de demo | Integrante 3 (presenta) e integrante 4 (maneja la terminal) | Cuesta US$0.0004 por medidor al mes y funciona de punta a punta con datos reales |
| **4 · Resultados y cierre** | 13 Resultados · 14 Conclusiones · 15 Próximos pasos | 2 min | Integrante 4 | La tarifa dinámica recorta 5.6 % la punta; se pide aprobar la arquitectura, ≈ US$36 k el primer año y un piloto en 2 zonas |

## Qué decir en cada diapositiva

| # | Diapositiva | Mensaje en una frase |
|---|---|---|
| 1 | Portada | Lumina une a doce distribuidoras y tiene lecturas de 8 millones de medidores que hoy solo sirven para facturar |
| 2 | Resumen ejecutivo | Una plataforma común con cuatro capacidades: tiempo real, plan diario, informes y mantenimiento |
| 3 | Situación actual | +39 % de carga en hora punta, 89 % de clientes con medidor y 12 distribuidoras que no comparten datos |
| 4 | Criterios de éxito | Cada indicador tiene un objetivo y una medición real del prototipo |
| 5 | Productos | Kafka, Spark, Airflow y Parquet: estándares abiertos que migran de nube cambiando solo la configuración |
| 6 | Supuestos | El supuesto más sensible es la prima de emergencia; por eso se propone un piloto antes del despliegue total |
| 7 | Arquitectura | Las lecturas entran por Kafka, Spark las evalúa en segundos y las guarda; el batch nocturno las vuelve historia |
| 8 | Patrón Lambda | Del batch al streaming viajan reglas; del streaming al batch, datos. Los cuatro puntos de sincronización se probaron en vivo |
| 9 | Resiliencia | Replicar no es respaldar: la segunda región cubre la caída de una región y los respaldos inmutables cubren los borrados |
| 10 | Calculadora | ≈ US$36 k el primer año, y el costo por lectura baja al crecer |
| 11 | Dos aplicaciones | Primero el usuario y su objetivo, después la pantalla: el cliente quiere pagar menos y Lumina quiere operar la red |
| 12 | Demo | Seguir el [guion de la demo](08_guion_demo.md) |
| 13 | Resultados | −5.6 % en la punta, 4.4 % de error en el pronóstico, 62 % de las fallas revisando el 5 % de los medidores |
| 14 | Conclusiones | Factible, barata, con valor medible y probada en vivo: la prueba encontró cinco fallas y quedaron corregidas |
| 15 | Próximos pasos | Cerrar con la decisión que se pide al comité |

## Preguntas probables y quién responde

| Pregunta | Bloque | Respuesta corta |
|---|---|---|
| ¿Por qué Lambda y no Kappa? | 2 | Recalcular dos años de perfiles y pronósticos es más barato y simple en batch |
| ¿Por qué no usaron Kafka gestionado, Composer y Bigtable en el prototipo? | 2 | Cobran por hora aunque nadie los use (≈ US$975 al mes juntos); la VM cuesta US$0.13 por hora y se destruye. El código es el mismo |
| ¿Qué pasa si se cae un concentrador? | 3 | Spark lo detecta en 2 horas por la falta de lecturas; en la prueba detectó los 52 medidores. El medidor guarda sus lecturas y no se pierde consumo |
| ¿De dónde sale el beneficio de US$9.6 M? | 4 | Del recorte medido de 5.6 % en la punta, con una prima supuesta de US$100/MWh. Aun con un décimo del efecto, la plataforma se paga sola |
| ¿El modelo de mantenimiento es mejor que una regla simple? | 4 | Con el mismo número de inspecciones empata; su valor es ir más allá de esa lista, donde acierta 3 veces más que al azar |
| ¿Cuánto costó el prototipo? | 3 | ≈ US$2.3 de los US$300 |

## Antes del 21

1. Repartir los bloques y que cada integrante lea las notas de sus diapositivas.
2. Ensayar una vez completo, con la demo en vivo, y grabar un video de respaldo.
3. Revisar que todos puedan abrir la presentación y las dos aplicaciones desde su computadora.
