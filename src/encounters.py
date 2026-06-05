from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.geo import haversine_km, local_x_km, local_y_km
from src.settings import (
    COLLISION_RADIUS_METERS, GRID_SIZE_METERS, MAX_CANDIDATES_TO_VALIDATE,
    TIME_BUCKET_SECONDS, TIME_TOLERANCE_SECONDS,
)


def add_join_bins(df: DataFrame) -> DataFrame:
    grid_km = GRID_SIZE_METERS / 1000.0
    return (
        df.withColumn("bucket_t", F.floor(F.col("timestamp").cast("long") / F.lit(TIME_BUCKET_SECONDS)).cast("long"))
        .withColumn("x_km", local_x_km(F.col("longitude")))
        .withColumn("y_km", local_y_km(F.col("latitude")))
        .withColumn("bucket_x", F.floor(F.col("x_km") / F.lit(grid_km)).cast("long"))
        .withColumn("bucket_y", F.floor(F.col("y_km") / F.lit(grid_km)).cast("long"))
    )


def expanded_keys(df: DataFrame) -> DataFrame:
    offsets = F.array(F.lit(-1), F.lit(0), F.lit(1))
    return (
        df.withColumn("dt", F.explode(offsets))
        .withColumn("dx", F.explode(offsets))
        .withColumn("dy", F.explode(offsets))
        .withColumn("join_t", F.col("bucket_t") + F.col("dt"))
        .withColumn("join_x", F.col("bucket_x") + F.col("dx"))
        .withColumn("join_y", F.col("bucket_y") + F.col("dy"))
        .drop("dt", "dx", "dy")
    )


def find_ranked_encounters(moving_df: DataFrame) -> DataFrame:
    base = add_join_bins(moving_df).select(
        "mmsi", "vessel_name", "timestamp", "latitude", "longitude", "sog", "cog", "bucket_t", "bucket_x", "bucket_y"
    )
    left = base.select(
        F.col("mmsi").alias("mmsi_a"),
        F.col("vessel_name").alias("name_a"),
        F.col("timestamp").alias("ts_a"),
        F.col("latitude").alias("lat_a"),
        F.col("longitude").alias("lon_a"),
        F.col("sog").alias("sog_a"),
        F.col("cog").alias("cog_a"),
        F.col("bucket_t").alias("join_t"),
        F.col("bucket_x").alias("join_x"),
        F.col("bucket_y").alias("join_y"),
    )
    right = expanded_keys(base).select(
        F.col("mmsi").alias("mmsi_b"),
        F.col("vessel_name").alias("name_b"),
        F.col("timestamp").alias("ts_b"),
        F.col("latitude").alias("lat_b"),
        F.col("longitude").alias("lon_b"),
        F.col("sog").alias("sog_b"),
        F.col("cog").alias("cog_b"),
        "join_t", "join_x", "join_y",
    )

    joined = left.join(right, ["join_t", "join_x", "join_y"], "inner").filter(F.col("mmsi_a") < F.col("mmsi_b"))
    timed = joined.withColumn("time_gap_seconds", F.abs(F.col("ts_a").cast("long") - F.col("ts_b").cast("long"))).filter(
        F.col("time_gap_seconds") <= F.lit(TIME_TOLERANCE_SECONDS)
    )
    measured = timed.withColumn("separation_m", haversine_km(F.col("lat_a"), F.col("lon_a"), F.col("lat_b"), F.col("lon_b")) * 1000.0).filter(
        F.col("separation_m") <= F.lit(COLLISION_RADIUS_METERS)
    )

    return (
        measured.withColumn("event_timestamp", F.least(F.col("ts_a"), F.col("ts_b")))
        .withColumn("event_latitude", (F.col("lat_a") + F.col("lat_b")) / 2.0)
        .withColumn("event_longitude", (F.col("lon_a") + F.col("lon_b")) / 2.0)
        .select(
            "mmsi_a", "name_a", "mmsi_b", "name_b", "event_timestamp", "event_latitude", "event_longitude",
            "ts_a", "ts_b", "time_gap_seconds", "separation_m", "lat_a", "lon_a", "lat_b", "lon_b", "sog_a", "sog_b", "cog_a", "cog_b",
        )
        .orderBy(F.col("separation_m").asc(), F.col("time_gap_seconds").asc(), F.col("event_timestamp").asc())
        .limit(MAX_CANDIDATES_TO_VALIDATE)
    )
