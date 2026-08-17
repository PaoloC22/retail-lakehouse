"""Fase 4 - Silver: products.
Cast numerico + enriquecimiento con category_translation (left join), con fallback a
"unknown" via coalesce para las ~610 categorias nulas y los nombres no traducidos.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from transform.silver_common import add_silver_audit_column, audit_new_nulls, cast_columns  # noqa: E402
from utils.spark_session import get_spark_session  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

BRONZE = Path("lakehouse/bronze")
SILVER = Path("lakehouse/silver")

SCHEMA = {
    "product_name_lenght": "int",
    "product_description_lenght": "int",
    "product_photos_qty": "int",
    "product_weight_g": "int",
    "product_length_cm": "int",
    "product_height_cm": "int",
    "product_width_cm": "int",
}

spark = get_spark_session("retail-lakehouse-silver-products")

bronze_df = spark.read.format("delta").load(str(BRONZE / "bronze_products"))
typed_df = cast_columns(bronze_df, SCHEMA)
audit_new_nulls(bronze_df, typed_df, SCHEMA.keys(), "products")

category = spark.read.format("delta").load(str(SILVER / "silver_category_translation")).select(
    "product_category_name", "product_category_name_english"
)

silver_df = (
    typed_df.join(category, on="product_category_name", how="left")
    .withColumn("product_category_name", F.coalesce(F.col("product_category_name"), F.lit("unknown")))
    .withColumn(
        "product_category_name_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )
)

unknown_count = silver_df.filter(F.col("product_category_name_english") == "unknown").count()
print(f"Productos con categoria 'unknown' tras el join: {unknown_count:,}")

silver_df = add_silver_audit_column(silver_df)

out_path = str(SILVER / "silver_products")
silver_df.write.format("delta").mode("overwrite").save(out_path)
print(f"silver_products: {silver_df.count():,} filas -> {out_path}")

spark.stop()
print("\nSILVER products COMPLETADO")
