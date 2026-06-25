from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

MATCH_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("match_id", IntegerType(), nullable=False),
        StructField("competition_code", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("utc_date", StringType(), nullable=True),
        StructField("matchday", IntegerType(), nullable=True),
        StructField("home_team_id", IntegerType(), nullable=True),
        StructField("home_team_name", StringType(), nullable=True),
        StructField("away_team_id", IntegerType(), nullable=True),
        StructField("away_team_name", StringType(), nullable=True),
        StructField("score_home", IntegerType(), nullable=True),
        StructField("score_away", IntegerType(), nullable=True),
        StructField("ingested_at", StringType(), nullable=True),
    ]
)