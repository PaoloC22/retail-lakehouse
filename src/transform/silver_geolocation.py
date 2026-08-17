"""Fase 4 - Silver: geolocation.
Colapsa ~1,000,163 filas (varias por zip_code_prefix) a 1 fila por zip:
- lat/lng: promedio (son numeros, tiene sentido)
- city/state: moda -- la combinacion mas frecuente por zip (via Window + row_number,
  mismo patron que usamos para deduplicar order_reviews)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from transform.silver_common import add_silver_audit_column, audit_new_nulls, cast_columns  # noqa: E402
from utils.spark_session import get_spark_session  # noqa: E402

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

BRONZE = Path("lakehouse/bronze")
SILVER = Path("lakehouse/silver")

SCHEMA = {
    "geolocation_zip_code_prefix": "int",
    "geolocation_lat": "double",
    "geolocation_lng": "double",
}

spark = get_spark_session("retail-lakehouse-silver-geolocation")

bronze_df = spark.read.format("delta").load(str(BRONZE / "bronze_geolocation"))
typed_df = cast_columns(bronze_df, SCHEMA)
audit_new_nulls(bronze_df, typed_df, SCHEMA.keys(), "geolocation")

# 1. Promedio de lat/lng por zip
coords = typed_df.groupBy("geolocation_zip_code_prefix").agg(
    F.avg("geolocation_lat").alias("geolocation_lat"),
    F.avg("geolocation_lng").alias("geolocation_lng"),
)

# 2. Moda de (city, state) por zip
city_state_counts = typed_df.groupBy(
    "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
).count()

window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("count"))
mode_city_state = (
    city_state_counts.withColumn("_rank", F.row_number().over(window))
    .filter(F.col("_rank") == 1)
    .select("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
)

silver_df = coords.join(mode_city_state, "geolocation_zip_code_prefix", "inner")

expected = typed_df.select("geolocation_zip_code_prefix").distinct().count()
actual = silver_df.count()
print(f"Zips distintos esperados: {expected:,} | filas en silver_geolocation: {actual:,}")
if expected != actual:
    raise ValueError("silver_geolocation no coincide con los zips distintos de Bronze -- revisar el join.")

silver_df = add_silver_audit_column(silver_df)
out_path = str(SILVER / "silver_geolocation")
silver_df.write.format("delta").mode("overwrite").save(out_path)
print(f"silver_geolocation: {actual:,} filas -> {out_path}")

# Completitud: zips que usan customers/sellers pero no existen en geolocation
customers = spark.read.format("delta").load(str(SILVER / "silver_customers"))
sellers = spark.read.format("delta").load(str(SILVER / "silver_sellers"))

orphan_customer_zips = (
    customers.join(
        silver_df,
        customers.customer_zip_code_prefix == silver_df.geolocation_zip_code_prefix,
        "left_anti",
    )
    .select("customer_zip_code_prefix")
    .distinct()
    .count()
)
orphan_seller_zips = (
    sellers.join(
        silver_df,
        sellers.seller_zip_code_prefix == silver_df.geolocation_zip_code_prefix,
        "left_anti",
    )
    .select("seller_zip_code_prefix")
    .distinct()
    .count()
)

print(f"Zips de customers sin match en geolocation: {orphan_customer_zips:,}")
print(f"Zips de sellers sin match en geolocation: {orphan_seller_zips:,}")

spark.stop()
print("\nSILVER geolocation COMPLETADO")
