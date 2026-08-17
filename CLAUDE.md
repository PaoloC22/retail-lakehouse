# Retail Lakehouse — ETL Pipeline & Lakehouse Architecture

Recreación guiada, paso a paso, del proyecto "ETL Pipeline & Lakehouse Architecture" del CV de Paolo Casas, usando el dataset público **Brazilian E-Commerce (Olist)** de Kaggle. El objetivo es repasar y volver a demostrar el dominio de: Python, PySpark, arquitectura Medallion (Bronze/Silver/Gold), ingesta batch de datos heterogéneos (CSV/JSON), transformaciones distribuidas y almacenamiento en Delta Lake.

## Dataset

Fuente: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Ya descargado en `data/raw/` (9 archivos CSV, ~99k órdenes, modelo relacional tipo copo de nieve):

| Archivo | Rows aprox. | Llave principal | Notas |
|---|---|---|---|
| `olist_orders_dataset.csv` | 99,441 | `order_id` | fechas de compra/aprobación/entrega |
| `olist_order_items_dataset.csv` | 112,650 | `order_id` + `order_item_id` | producto, seller, precio, flete |
| `olist_order_payments_dataset.csv` | 103,886 | `order_id` + `payment_sequential` | tipo de pago, cuotas, monto |
| `olist_order_reviews_dataset.csv` | 104,719 | `review_id` | score y comentarios de reseñas |
| `olist_customers_dataset.csv` | 99,441 | `customer_id` | `customer_unique_id`, zip, ciudad, estado |
| `olist_products_dataset.csv` | 32,951 | `product_id` | categoría, dimensiones, peso |
| `olist_sellers_dataset.csv` | 3,095 | `seller_id` | zip, ciudad, estado |
| `olist_geolocation_dataset.csv` | 1,000,163 | `geolocation_zip_code_prefix` | lat/lng por prefijo de zip (muchos duplicados) |
| `product_category_name_translation.csv` | 71 | `product_category_name` | traducción PT → EN de categorías |

Para cubrir el bullet del CV de "ingesta de datos heterogéneos (JSON/CSV)" vamos a **convertir 1-2 de estas fuentes a JSON** (ej. `order_reviews` y `product_category_name_translation`) antes de la ingesta Bronze, simulando un origen distinto (API/export) junto a los CSV nativos.

## Stack técnico

- **Entorno: conda env `pyspark_env`** (ya existía, creado con Anaconda) — Python 3.11.15. Se reutiliza, no se crea un venv nuevo.
  - Activar en una terminal nueva con: `conda activate pyspark_env`
- **PySpark 4.1.1** + **delta-spark 4.3.1** (Delta Lake) + pandas + pytest — instalados en `pyspark_env`. Ver `requirements.txt`.
  - Nota: `delta-spark` fijó la versión de pyspark en `<=4.1.1` (downgrade automático desde el 4.2.0 que traía el env por defecto). Es esperado y correcto.
- **Java 17** (Temurin), ya instalado a nivel de sistema — compatible con Spark 4.1.x.
- **Windows + winutils.exe**: PySpark en Windows requiere `winutils.exe`/`hadoop.dll` (Hadoop 3.4.2) para inicializar `SparkContext`. Ya están en `C:\hadoop\bin\` y `HADOOP_HOME=C:\hadoop` está seteado como variable de usuario (persistida en registro `HKCU:\Environment`). Cualquier terminal nueva la ve automáticamente.
- **Gotcha de ejecución**: invocar el intérprete directamente (`C:\Users\LENOVO\anaconda3\envs\pyspark_env\python.exe script.py`) en vez de `conda run -n pyspark_env python script.py` — con `conda run` los workers de PySpark fallaban con `Python worker failed to connect back` (timeout). También conviene fijar explícitamente `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` a esa misma ruta de `python.exe` antes de correr cualquier script.
- **Docker**: no instalado todavía en la máquina; lo instalamos en la fase de containerización si se desea acercar al despliegue real del CV.
- Réplica opcional final en **Databricks Community Edition** (gratis) para fidelidad 1:1 con el CV, una vez el pipeline funcione local.

## Arquitectura Medallion

```
data/raw/ (CSV originales)
   │
   ▼  simular fuente JSON para 1-2 tablas
data/landing/ (csv + json mezclados, como llegarían "crudos")
   │
   ▼  Bronze: ingesta 1:1, sin transformar, + metadata de ingesta
lakehouse/bronze/*.delta   (schema-on-read laxo, columnas: _ingestion_ts, _source_file)
   │
   ▼  Silver: limpieza, tipado, dedupe, nulos, normalización, joins de referencia
lakehouse/silver/*.delta   (tipos correctos, sin duplicados, claves validadas)
   │
   ▼  Gold: modelado dimensional + agregados de negocio
lakehouse/gold/*.delta     (star schema: fact_orders + dimensiones, KPIs)
```

## Estructura de carpetas objetivo

```
retail-lakehouse/
  data/
    raw/          # CSV originales de Kaggle (ya está)
    landing/      # copia + versión JSON de 1-2 fuentes (simula heterogeneidad)
  lakehouse/
    bronze/
    silver/
    gold/
  src/
    ingestion/    # scripts de carga Bronze (csv + json)
    transform/    # scripts Silver (limpieza) y Gold (modelado + KPIs)
    utils/        # spark session factory, schemas, logging
  tests/          # pytest sobre funciones de transformación
  notebooks/      # exploración/EDA puntual
  docker/         # Dockerfile + docker-compose (fase final, opcional)
  requirements.txt
  README.md
  CLAUDE.md
```

## Hallazgos EDA (Fase 1) y decisiones de limpieza para Silver

Script: `src/eda/eda_olist.py`. Resultados completos en el output de la corrida (ver historial); resumen y reglas que se derivan:

| Hallazgo | Tabla | Decisión para Silver |
|---|---|---|
| 814 `review_id` duplicados (99,224 filas / 98,410 PK únicas) | order_reviews | Dedupe quedándose con el registro de `review_answer_timestamp` más reciente por `review_id` |
| 1.85% de productos sin `product_category_name` (~610) | products | Categoría → `"unknown"` en vez de null; no descartar la fila (el producto sigue siendo válido para ventas) |
| 623 FKs huérfanas `products.product_category_name → category_translation` | products / category_translation | Left join con fallback a `"unknown"` cuando no hay traducción (cubre nulos + nombres no traducidos) |
| `geolocation` sin PK propia, múltiples lat/lng por zip prefix | geolocation | Agregar a 1 fila por `zip_code_prefix` (promedio lat/lng, moda de ciudad/estado) antes de usarla como dimensión |
| Nulos en `order_delivered_carrier_date` (1.79%) / `order_delivered_customer_date` (2.98%) / `order_approved_at` (0.16%) | orders | Esperado (orden aún no llegó a esa etapa) — **no imputar**, dejar null y derivar flags booleanos (`is_delivered`, etc.) en Silver/Gold |
| Nulos en `review_comment_title` (88.3%) / `review_comment_message` (58.7%) | order_reviews | Esperado (reseña sin texto) — dejar null, no imputar |
| `order_status`: 97% `delivered`, resto repartido (shipped/canceled/unavailable/invoiced/processing/created/approved) | orders | Gold calcula KPIs de ventas/entrega **solo sobre `delivered`**, pero mantiene el conteo total de órdenes por status como métrica aparte |
| 0 órdenes con `order_delivered_customer_date < order_purchase_timestamp` | orders | Sin inconsistencia temporal, no requiere corrección |
| Rango de datos: 2016-09-04 a 2018-10-17 | orders | Referencia para particionar Gold por año/mes |
| `review_score` sesgado a positivo (57% = 5, 19% = 4) | order_reviews | Anotar como contexto de negocio, no es un problema de calidad de datos |

## Roadmap paso a paso

- [ ] **Fase 0 — Entorno**: crear venv, `requirements.txt` (pyspark, delta-spark, pandas, pytest), verificar `py -m pyspark` levanta correctamente con Java 17, `git init` del proyecto.
- [x] **Fase 1 — Exploración (EDA)**: entender el modelo relacional Olist, calidad de datos (nulos, duplicados, rangos de fechas), diccionario de datos. Ver sección "Hallazgos EDA" arriba.
- [x] **Fase 2 — Landing heterogénea**: `src/ingestion/build_landing_zone.py` (Pandas, `to_json(orient="records", lines=True)` → NDJSON) convierte `order_reviews` y `category_translation` a JSON; el resto se copia como CSV. Resultado en `data/landing/` (7 CSV + 2 JSON). Nota: este script es *setup* para simular la fuente heterogénea, no es parte del pipeline productivo — el pipeline real arranca en Bronze (Fase 3).
- [x] **Fase 3 — Capa Bronze**: `src/ingestion/bronze_ingest.py` lee `data/landing/` (CSV sin `inferSchema` + JSON con `primitivesAsString=True` → todo queda `string`, fidelidad total) y escribe 9 tablas Delta en `lakehouse/bronze/bronze_<entidad>`. Columnas de auditoría: `_source_file`, `_source_format` (csv/json), `_ingestion_timestamp` (un solo timestamp por corrida/batch). Sin particionar (carga histórica única). Conteos verificados 1:1 contra landing, sin pérdida de datos. `SparkSession` con Delta factorizada en `src/utils/spark_session.py` (reusable para Silver/Gold).
- [x] **Fase 4 — Capa Silver**: 9 tablas Delta en `lakehouse/silver/`, todas leyendo desde Bronze (nunca desde raw/landing). Patrón usado en todos los scripts: cast tipado vía diccionario `{columna: tipo}` (`src/transform/silver_common.py`) + auditoría de nulos nuevos post-cast (compara nulos antes/después, corta si el cast introdujo pérdida silenciosa de datos).
  - `orders`, `order_items`, `order_payments`, `customers`, `sellers`: solo tipado (`src/transform/silver_simple_tables.py`).
  - `order_reviews` (`src/transform/silver_order_reviews.py`): **sin dedupe por `review_id`**. El EDA marcó 814 "duplicados", pero se descubrió que `(review_id, order_id)` es 100% único — la misma reseña a veces aplica a más de un pedido (dato real de Olist, no error). Se cambió el grano a `(review_id, order_id)` para no perder la asociación al pedido en los joins de Gold; incluye un check defensivo que revienta el script si esa unicidad deja de cumplirse.
  - `geolocation` (`src/transform/silver_geolocation.py`): colapsado de 1,000,163 filas a 19,015 (una por `zip_code_prefix`) — lat/lng por promedio, ciudad/estado por moda (`Window` + `row_number()`, mismo patrón que el dedupe). Detectado: 157 zips de `customers` y 7 de `sellers` sin match en `geolocation` — quedará como `NULL` en los joins de Gold, es una limitación real del dataset, no se fuerza a completar.
  - `category_translation` (`src/transform/silver_category_translation.py`): passthrough sin cambios, para que `products` lea Silver→Silver.
  - `products` (`src/transform/silver_products.py`): cast numérico + left join con `silver_category_translation`, `product_category_name` y `product_category_name_english` con fallback `"unknown"` vía `F.coalesce()`. Confirmado: 623 productos quedaron en `"unknown"`, coincide exacto con los huérfanos del EDA.
- [ ] **Fase 5 — Capa Gold**: modelado dimensional (fact_orders, dim_customer, dim_product, dim_seller, dim_date) + tablas de KPIs: ventas por categoría/estado/mes, performance de entrega (tiempo estimado vs real), ticket promedio, ranking de vendedores, distribución de reviews.
- [ ] **Fase 6 — Orquestación**: script end-to-end que corre Bronze→Silver→Gold en orden, logging y manejo de errores por etapa.
- [ ] **Fase 7 — Testing y calidad**: pytest para funciones de transformación clave, validaciones de esquema/conteos entre capas.
- [ ] **Fase 8 — Containerización**: Dockerfile con PySpark + Delta, `docker-compose` si se agrega algo adicional (ej. Jupyter).
- [ ] **Fase 9 — Documentación**: README con diagrama de arquitectura, diagrama ER, instrucciones de ejecución.
- [ ] **Fase 10 (opcional)** — Réplica en Databricks Community Edition para igualar el entorno original del CV.

## Convenciones

- Nombres de tablas Delta: `bronze_<fuente>`, `silver_<entidad>`, `gold_<fact|dim>_<nombre>`.
- Toda escritura a Delta usa modo `overwrite` con `mergeSchema` explícito solo cuando aplique (evitar sorpresas de esquema).
- Cada capa se puede recorrer de forma independiente leyendo la capa anterior — no se salta de Bronze a Gold directo.
- Código en `src/`, notebooks solo para exploración puntual, no para lógica de producción del pipeline.

## Estado actual

- [x] **Fase 0 — Entorno**: `pyspark_env` (conda, Python 3.11.15) con PySpark 4.1.1 + delta-spark 4.3.1 + pandas + pytest instalados. `winutils.exe`/`hadoop.dll` configurados en `C:\hadoop\bin`, `HADOOP_HOME` seteado. Smoke test (`src/utils/smoke_test.py`) confirmó que Spark levanta y escribe/lee tablas Delta correctamente (`SMOKE TEST OK`).
- [ ] Falta: `git init` del proyecto (pendiente, opcional).

- [x] **Fase 1 — EDA**: completada, ver hallazgos y decisiones de limpieza arriba.
- [x] **Fase 2 — Landing heterogénea**: completada, `data/landing/` con 7 CSV + 2 JSON (NDJSON).
- [x] **Fase 3 — Capa Bronze**: completada, 9 tablas Delta en `lakehouse/bronze/`, todo string + auditoría.
- [x] **Fase 4 — Capa Silver**: completada, 9 tablas Delta en `lakehouse/silver/`. Ver detalle arriba (incluye un hallazgo que cambió el plan original: `order_reviews` no se dedupe por `review_id`, se cambió el grano a `(review_id, order_id)`).

Dataset ya en `data/raw/` (9 CSV de Olist).

Modo de trabajo desde Fase 2: **socrático** — explicar el concepto y hacer preguntas guía antes de escribir código, luego construir juntos con base en las respuestas.

**Cambio de modo (2026-08-16):** Paolo está preparándose para una entrevista de Data Engineer en **AJE Group (unidad Apex Digital)**. Desde aquí, **Paolo escribe el código él mismo**; Claude plantea el requerimiento (como un ticket), da pistas/recuerda sintaxis cuando se traba, y revisa — ya no escribe la solución de entrada. Ver memoria `project-aje-interview-prep` / `feedback-learning-mode`.

## Checklist técnico de la entrevista (de José Paredes, referido) y dónde se practica

| Tema | Dónde se practica |
|---|---|
| PySpark / SQL, particionamiento, filtros, condicionales | Ya cubierto en Bronze/Silver (Fases 3-4); Gold (Fase 5) lo profundiza con particionamiento explícito por fecha |
| Write modes: overwrite / append / merge (upsert) | Solo usamos `overwrite` hasta ahora — pendiente ejercicio dedicado de `append` (carga incremental simulada) y `MERGE INTO` (upsert) con Delta Lake |
| Explosión de datos (`explode`) | No ocurre naturalmente en Olist (no hay columnas array/nested) — pendiente ejercicio dedicado con datos sintéticos |
| Git: ramas, ambientes (prd/qa/dev), CI/CD | Proyecto aún no es repo git — **primer ejercicio pendiente** |
| MongoDB (NoSQL) | No cubierto — pendiente ejercicio dedicado |
| Databricks → Azure | Estamos corriendo todo local (`pyspark_env` + Delta local); Databricks Community Edition / Azure requieren que Paolo lo haga directamente (fuera de esta sesión) — Claude puede guiar el traspaso de los scripts cuando llegue el momento |

Próximo paso: **Fase 5 — Capa Gold**, con Paolo codeando y aplicando particionamiento + write modes variados. Antes de eso, ejercicio de Git (ramas dev/qa/prd).
