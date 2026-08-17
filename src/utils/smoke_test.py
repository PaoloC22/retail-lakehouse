"""Verifica que PySpark + Delta Lake levantan correctamente en el entorno pyspark_env."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("retail-lakehouse-smoke-test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"])
df.write.format("delta").mode("overwrite").save("data/_smoke_test_delta")

read_back = spark.read.format("delta").load("data/_smoke_test_delta")
read_back.show()

print("PySpark version:", spark.version)
print("SMOKE TEST OK")
spark.stop()
