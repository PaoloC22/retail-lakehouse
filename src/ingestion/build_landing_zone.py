"""Fase 2 - Fabrica la landing zone: simula que order_reviews y category_translation
llegaron como JSON (NDJSON) desde una fuente externa, mientras el resto sigue en CSV.
No es parte del pipeline productivo (eso empieza en Bronze, Fase 3) -- esto es setup.
"""
import shutil
from pathlib import Path

import pandas as pd

RAW = Path("data/raw")
LANDING = Path("data/landing")
LANDING.mkdir(parents=True, exist_ok=True)

# Fuentes que se van a "reescribir" como si llegaran en JSON (NDJSON)
JSON_SOURCES = {
    "olist_order_reviews_dataset.csv": "order_reviews.json",
    "product_category_name_translation.csv": "category_translation.json",
}

for csv_name, json_name in JSON_SOURCES.items():
    df = pd.read_csv(RAW / csv_name)
    df.to_json(
        LANDING / json_name,
        orient="records",
        lines=True,       # NDJSON: un objeto por linea, splittable para Spark
        date_format="iso",
        force_ascii=False,
    )
    print(f"{csv_name} -> {json_name} ({len(df):,} filas)")

# El resto de fuentes llega "tal cual" en CSV, como en la fuente original
for csv_path in RAW.glob("*.csv"):
    if csv_path.name in JSON_SOURCES:
        continue
    shutil.copy(csv_path, LANDING / csv_path.name)
    print(f"{csv_path.name} -> copiado sin cambios")

print("\nLANDING ZONE LISTA en data/landing/")
