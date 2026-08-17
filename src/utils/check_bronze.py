"""Verificacion rapida de una tabla Bronze: schema (todo string + auditoria) y muestra de filas."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("check-bronze")
df = spark.read.format("delta").load("lakehouse/bronze/bronze_order_reviews")
df.printSchema()
df.select("_source_file", "_source_format", "_ingestion_timestamp").show(2, truncate=False)
spark.stop()
