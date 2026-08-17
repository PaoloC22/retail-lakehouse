"""Fase 1 - EDA del dataset Olist: schemas, duplicados, nulos, integridad referencial y fechas."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("retail-lakehouse-eda")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

RAW = "data/raw"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def read_csv(name):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW}/{name}")
    )


dfs = {k: read_csv(v) for k, v in TABLES.items()}


def section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


# 1. Schemas + conteo de filas
for name, df in dfs.items():
    section(f"TABLA: {name}")
    df.printSchema()
    print(f"Filas: {df.count():,}")

# 2. Duplicados por llave primaria declarada
PK = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"],
}

section("DUPLICADOS POR LLAVE PRIMARIA (geolocation no tiene PK propia, se omite)")
for name, keys in PK.items():
    df = dfs[name]
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    print(f"{name}: filas={total:,} | pk distintas={distinct:,} | duplicadas={total - distinct:,}")

# 3. Nulos por columna
section("NULOS POR COLUMNA (% del total, solo columnas con nulos)")
for name, df in dfs.items():
    total = df.count()
    null_counts = df.select(
        [F.round(F.sum(F.col(c).isNull().cast("int")) / total * 100, 2).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    non_zero = {k: v for k, v in null_counts.items() if v and v > 0}
    if non_zero:
        print(f"\n{name}:")
        for col, pct in sorted(non_zero.items(), key=lambda x: -x[1]):
            print(f"  {col}: {pct}%")
    else:
        print(f"\n{name}: sin nulos")

# 4. Integridad referencial (FKs huérfanas)
section("INTEGRIDAD REFERENCIAL (FKs huérfanas)")
checks = [
    ("order_items.order_id -> orders.order_id", "order_items", "order_id", "orders", "order_id"),
    ("order_items.product_id -> products.product_id", "order_items", "product_id", "products", "product_id"),
    ("order_items.seller_id -> sellers.seller_id", "order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments.order_id -> orders.order_id", "order_payments", "order_id", "orders", "order_id"),
    ("order_reviews.order_id -> orders.order_id", "order_reviews", "order_id", "orders", "order_id"),
    ("orders.customer_id -> customers.customer_id", "orders", "customer_id", "customers", "customer_id"),
    (
        "products.product_category_name -> category_translation.product_category_name",
        "products",
        "product_category_name",
        "category_translation",
        "product_category_name",
    ),
]
for label, lname, lcol, rname, rcol in checks:
    left, right = dfs[lname], dfs[rname]
    orphans = left.join(right, left[lcol] == right[rcol], "left_anti").count()
    print(f"{label}: huerfanas={orphans:,}")

# 5. Distribución de order_status
section("DISTRIBUCION DE order_status")
dfs["orders"].groupBy("order_status").count().orderBy(F.desc("count")).show(truncate=False)

# 6. Rangos de fecha y consistencia temporal
section("RANGOS DE FECHA Y CONSISTENCIA TEMPORAL (orders)")
date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
dfs["orders"].select([F.min(c).alias(f"min_{c}") for c in date_cols]).show(truncate=False)
dfs["orders"].select([F.max(c).alias(f"max_{c}") for c in date_cols]).show(truncate=False)

inconsistent = dfs["orders"].filter(
    F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp")
).count()
print(f"Ordenes entregadas ANTES de la fecha de compra (inconsistencia): {inconsistent}")

# 7. Distribución de review_score
section("DISTRIBUCION DE review_score")
dfs["order_reviews"].groupBy("review_score").count().orderBy("review_score").show()

spark.stop()
print("\nEDA COMPLETADO")
