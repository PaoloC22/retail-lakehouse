"""Fase 4 - Silver: tipado de las tablas "simples" (orders, order_items, order_payments,
customers, sellers) -- sin dedupe ni agregacion, esos casos van en scripts aparte.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from transform.silver_common import add_silver_audit_column, audit_new_nulls, cast_columns  # noqa: E402
from utils.spark_session import get_spark_session  # noqa: E402

BRONZE = Path("lakehouse/bronze")
SILVER = Path("lakehouse/silver")

# Solo se listan las columnas que necesitan cast; el resto se queda como string.
TABLE_SCHEMAS = {
    "orders": {
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp",
    },
    "order_items": {
        "order_item_id": "int",
        "shipping_limit_date": "timestamp",
        "price": "double",
        "freight_value": "double",
    },
    "order_payments": {
        "payment_sequential": "int",
        "payment_installments": "int",
        "payment_value": "double",
    },
    "customers": {
        "customer_zip_code_prefix": "int",
    },
    "sellers": {
        "seller_zip_code_prefix": "int",
    },
}

spark = get_spark_session("retail-lakehouse-silver-simple")

for table_name, schema in TABLE_SCHEMAS.items():
    bronze_df = spark.read.format("delta").load(str(BRONZE / f"bronze_{table_name}"))
    silver_df = cast_columns(bronze_df, schema)
    audit_new_nulls(bronze_df, silver_df, schema.keys(), table_name)
    silver_df = add_silver_audit_column(silver_df)

    out_path = str(SILVER / f"silver_{table_name}")
    silver_df.write.format("delta").mode("overwrite").save(out_path)
    print(f"silver_{table_name}: {silver_df.count():,} filas -> {out_path}")

spark.stop()
print("\nSILVER (tablas simples) COMPLETADO")
