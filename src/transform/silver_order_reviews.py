"""Fase 4 - Silver: order_reviews.
Grano: (review_id, order_id) -- confirmado unico (99,224 filas = 99,224 combinaciones).
No se deduplica por review_id solo: la misma resenia puede aplicar legitimamente a
mas de un pedido, y descartarla rompe el join order-level en Gold.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from transform.silver_common import add_silver_audit_column, audit_new_nulls, cast_columns  # noqa: E402
from utils.spark_session import get_spark_session  # noqa: E402

BRONZE = Path("lakehouse/bronze")
SILVER = Path("lakehouse/silver")

SCHEMA = {
    "review_score": "int",
    "review_creation_date": "timestamp",
    "review_answer_timestamp": "timestamp",
}

spark = get_spark_session("retail-lakehouse-silver-order-reviews")

bronze_df = spark.read.format("delta").load(str(BRONZE / "bronze_order_reviews"))
silver_df = cast_columns(bronze_df, SCHEMA)
audit_new_nulls(bronze_df, silver_df, SCHEMA.keys(), "order_reviews")

# Verificacion defensiva del grano: si algun dia deja de ser unico, el pipeline debe
# fallar aqui en vez de escribir una tabla Silver silenciosamente incorrecta.
total = silver_df.count()
distinct_composite = silver_df.select("review_id", "order_id").distinct().count()
if total != distinct_composite:
    raise ValueError(
        f"(review_id, order_id) dejo de ser unico: {total:,} filas vs {distinct_composite:,} combinaciones."
        " Revisar la logica de grano antes de continuar."
    )
print(f"Grano (review_id, order_id) verificado: {total:,} filas, todas unicas")

silver_df = add_silver_audit_column(silver_df)

out_path = str(SILVER / "silver_order_reviews")
silver_df.write.format("delta").mode("overwrite").save(out_path)
print(f"silver_order_reviews: {silver_df.count():,} filas -> {out_path}")

spark.stop()
print("\nSILVER order_reviews COMPLETADO")
