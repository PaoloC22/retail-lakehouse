"""Verifica si (review_id, order_id) es una llave compuesta unica en order_reviews."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("check-review-composite-key")
df = spark.read.format("delta").load("lakehouse/bronze/bronze_order_reviews")

total = df.count()
distinct_composite = df.select("review_id", "order_id").distinct().count()
distinct_review_id = df.select("review_id").distinct().count()

print(f"Filas totales: {total:,}")
print(f"(review_id, order_id) distintos: {distinct_composite:,}")
print(f"review_id distintos: {distinct_review_id:,}")
print(f"Duplicados sobre (review_id, order_id): {total - distinct_composite:,}")

spark.stop()
