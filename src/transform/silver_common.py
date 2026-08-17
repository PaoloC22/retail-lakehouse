"""Utilidades reusadas por los scripts de Silver: cast tipado + auditoria de nulos nuevos."""
from datetime import datetime, timezone

from pyspark.sql import functions as F


def cast_columns(df, schema: dict):
    """Aplica .cast(tipo) a cada columna indicada en schema, deja el resto intacto."""
    for col_name, target_type in schema.items():
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def audit_new_nulls(before_df, after_df, columns, table_name):
    """Compara nulos antes/despues del cast para detectar valores que no se pudieron convertir."""
    print(f"\nAuditoria de nulos nuevos por cast -- {table_name}")
    any_new = False
    for col_name in columns:
        before_nulls = before_df.filter(F.col(col_name).isNull()).count()
        after_nulls = after_df.filter(F.col(col_name).isNull()).count()
        diff = after_nulls - before_nulls
        if diff > 0:
            any_new = True
            print(f"  ALERTA {col_name}: {before_nulls} -> {after_nulls} nulos (+{diff} por cast fallido)")
    if not any_new:
        print("  OK: el cast no introdujo nulos nuevos en ninguna columna")


def add_silver_audit_column(df):
    ts = datetime.now(timezone.utc).isoformat()
    return df.withColumn("_silver_processed_timestamp", F.lit(ts).cast("timestamp"))
