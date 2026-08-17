"""Fase 4 - Silver: category_translation. Tabla de referencia pequenia, sin casts necesarios."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from transform.silver_common import add_silver_audit_column  # noqa: E402
from utils.spark_session import get_spark_session  # noqa: E402

BRONZE = Path("lakehouse/bronze")
SILVER = Path("lakehouse/silver")

spark = get_spark_session("retail-lakehouse-silver-category-translation")

df = spark.read.format("delta").load(str(BRONZE / "bronze_category_translation"))
df = add_silver_audit_column(df)

out_path = str(SILVER / "silver_category_translation")
df.write.format("delta").mode("overwrite").save(out_path)
print(f"silver_category_translation: {df.count():,} filas -> {out_path}")

spark.stop()
