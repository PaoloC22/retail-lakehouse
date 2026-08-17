"""Verifica si los 814 review_id duplicados tienen empate exacto en review_answer_timestamp,
y si las filas duplicadas son copias exactas o difieren en algun otro campo."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_spark_session("check-review-ties")
df = spark.read.format("delta").load("lakehouse/bronze/bronze_order_reviews")
cols = [c for c in df.columns if not c.startswith("_")]  # sin columnas de auditoria

dup_ids = df.groupBy("review_id").count().filter(F.col("count") > 1).select("review_id")
dup_rows = df.join(dup_ids, "review_id").select(*cols)

other_cols = [c for c in cols if c != "review_id"]
row_signature = F.concat_ws(
    "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<<NULL>>")) for c in other_cols]
)
dup_rows = dup_rows.withColumn("_row_signature", row_signature)

exact_dupe_groups = (
    dup_rows.groupBy("review_id")
    .agg(F.countDistinct("_row_signature").alias("distinct_rows"), F.count("*").alias("n"))
)

print("Grupos donde TODAS las columnas (menos review_id) son identicas:",
      exact_dupe_groups.filter(F.col("distinct_rows") == 1).count())
print("Grupos donde difiere algo mas que el review_id:",
      exact_dupe_groups.filter(F.col("distinct_rows") > 1).count())

print("\nEjemplo de un grupo que SI difiere en algo (si existe):")
example_id = (
    exact_dupe_groups.filter(F.col("distinct_rows") > 1).select("review_id").limit(1).collect()
)
if example_id:
    dup_rows.filter(F.col("review_id") == example_id[0]["review_id"]).show(truncate=60)

spark.stop()
