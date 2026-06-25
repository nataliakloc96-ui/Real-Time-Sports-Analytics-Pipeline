from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine

SILVER_PATH = "../spark/data/silver/match_events"

POSTGRES_URI = "postgresql+psycopg://sports:sports_dev_password@localhost:5434/sports_analytics"

TARGET_SCHEMA = "raw"
TARGET_TABLE = "match_events"

def main() -> None:
    print(f"-> Wczytuję Parquet z: {SILVER_PATH}")
    df = pd.read_parquet(SILVER_PATH)
    print(f"Wczytano {len(df)} wierszy, {len(df.columns)} kolumn")

    engine = create_engine(
        "postgresql+psycopg://sports@127.0.0.1:5434/sports_analytics",
        
    )

    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    
    print(f"-> Zapisuję do Postgres: {TARGET_SCHEMA}.{TARGET_TABLE}")
    df.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )

    print(f"Załadowano {len(df)} wierszy do {TARGET_SCHEMA}.{TARGET_TABLE}")

if __name__ == "__main__":
    main()