from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.settings import END_TS, INPUT_GLOB, START_TS

COLUMN_ALIASES = {
    "# Timestamp": "timestamp_text",
    "Timestamp": "timestamp_text",
    "MMSI": "mmsi",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Navigational status": "nav_status",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "Name": "vessel_name",
    "Ship type": "ship_type",
}

OUTPUT_COLUMNS = [
    "timestamp", "mmsi", "latitude", "longitude", "nav_status",
    "sog", "cog", "heading", "vessel_name", "ship_type",
]


def read_csvs(spark: SparkSession, path_glob: str = INPUT_GLOB) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("recursiveFileLookup", True)
        .csv(path_glob)
    )


def standardise_columns(df: DataFrame) -> DataFrame:
    for source, target in COLUMN_ALIASES.items():
        if source in df.columns:
            df = df.withColumnRenamed(source, target)
    for col_name in set(COLUMN_ALIASES.values()):
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df


def parse_ais(df: DataFrame) -> DataFrame:
    df = standardise_columns(df)
    # Danish AIS files usually use dd/MM/yyyy HH:mm:ss, but coalesce makes the parser robust to ISO-like files.
    timestamp = F.coalesce(
        F.to_timestamp("timestamp_text", "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp("timestamp_text", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("timestamp_text"),
    )
    parsed = (
        df.withColumn("timestamp", timestamp)
        .withColumn("mmsi", F.col("mmsi").cast("long"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .withColumn("cog", F.col("cog").cast("double"))
        .withColumn("heading", F.col("heading").cast("double"))
    )
    return parsed.select(*OUTPUT_COLUMNS)


def load_assignment_month(spark: SparkSession) -> DataFrame:
    return parse_ais(read_csvs(spark)).filter(
        (F.col("timestamp") >= F.lit(START_TS)) & (F.col("timestamp") < F.lit(END_TS))
    )
