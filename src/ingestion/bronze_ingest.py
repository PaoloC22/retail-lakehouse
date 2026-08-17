"""Fase 3 - Capa Bronze: ingesta 1:1 de data/landing/ (CSV+JSON) a Delta Lake.
Todo se lee como string (fidelidad total con la fuente) + columnas de auditoria.
El tipado y la limpieza de valores se hacen recien en Silver.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # habilita "from utils..."
from utils.spark_session import get_spark_session  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

LANDING = Path("data/landing")
BRONZE = Path("lakehouse/bronze")

CSV_SOURCES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}

JSON_SOURCES = {
    "order_reviews": "order_reviews.json",
    "category_translation": "category_translation.json",
}

spark = get_spark_session("retail-lakehouse-bronze")

# Un solo timestamp para todo el run: todas las filas de este batch quedan marcadas
# con el mismo instante de ingesta, como una "corrida" identificable.
ingestion_ts = datetime.now(timezone.utc).isoformat()


def add_audit_columns(df, source_file, source_format):
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_format", F.lit(source_format))
        .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    )


def write_bronze(df, table_name):
    out_path = str(BRONZE / f"bronze_{table_name}")
    df.write.format("delta").mode("overwrite").save(out_path)
    print(f"bronze_{table_name}: {df.count():,} filas -> {out_path}")


for table_name, file_name in CSV_SOURCES.items():
    df = (
        spark.read.option("header", True)
        # sin inferSchema: todo string, cero perdida silenciosa de datos crudos
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(LANDING / file_name))
    )
    df = add_audit_columns(df, file_name, "csv")
    write_bronze(df, table_name)

for table_name, file_name in JSON_SOURCES.items():
    df = (
        spark.read.option("primitivesAsString", True)  # equivalente a "sin inferSchema" para JSON
        .json(str(LANDING / file_name))
    )
    df = add_audit_columns(df, file_name, "json")
    write_bronze(df, table_name)

spark.stop()
print("\nBRONZE COMPLETADO")
