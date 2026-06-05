from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.geo import distance_from_center_km, haversine_km
from src.settings import (
    CENTER_LAT, CENTER_LON, MAX_IMPLIED_SOG_KNOTS, MAX_REPORTED_SOG_KNOTS, MAX_TRACK_GAP_SECONDS,
    MIN_DISPLACEMENT_KM, MIN_MOVING_SOG_KNOTS, SEARCH_RADIUS_KM,
)


def keep_valid_positions(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("timestamp").isNotNull()
        & F.col("mmsi").isNotNull()
        & F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
    )


def keep_assignment_area(df: DataFrame) -> DataFrame:
    # Fast latitude/longitude box first, then exact Haversine radius. This reduces the expensive trig work.
    lat_margin = SEARCH_RADIUS_KM / 110.574
    lon_margin = SEARCH_RADIUS_KM / (111.320 * __import__("math").cos(__import__("math").radians(CENTER_LAT)))
    boxed = df.filter(
        F.col("latitude").between(F.lit(CENTER_LAT - lat_margin), F.lit(CENTER_LAT + lat_margin))
        & F.col("longitude").between(F.lit(CENTER_LON - lon_margin), F.lit(CENTER_LON + lon_margin))
    )
    return boxed.withColumn("distance_to_center_km", distance_from_center_km(F.col("latitude"), F.col("longitude"))).filter(
        F.col("distance_to_center_km") <= F.lit(SEARCH_RADIUS_KM)
    )


def add_track_diagnostics(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("mmsi").orderBy("timestamp")
    with_prev = (
        df.dropDuplicates(["mmsi", "timestamp", "latitude", "longitude"])
        .withColumn("prev_ts", F.lag("timestamp").over(w))
        .withColumn("prev_lat", F.lag("latitude").over(w))
        .withColumn("prev_lon", F.lag("longitude").over(w))
        .withColumn("next_ts", F.lead("timestamp").over(w))
    )
    with_prev = with_prev.withColumn("dt_prev", F.col("timestamp").cast("long") - F.col("prev_ts").cast("long"))
    with_prev = with_prev.withColumn(
        "step_km",
        F.when(F.col("prev_lat").isNotNull(), haversine_km(F.col("latitude"), F.col("longitude"), F.col("prev_lat"), F.col("prev_lon"))),
    )
    return with_prev.withColumn(
        "implied_sog",
        F.when((F.col("dt_prev") > 0) & F.col("step_km").isNotNull(), F.col("step_km") / (F.col("dt_prev") / 3600.0) / 1.852),
    )


def remove_noise_and_stationary(df: DataFrame) -> DataFrame:
    nav = F.lower(F.coalesce(F.col("nav_status").cast("string"), F.lit("")))
    stationary_words = nav.contains("anchor") | nav.contains("moored") | nav.contains("aground") | nav.contains("not defined")

    plausible_reported_speed = F.col("sog").isNull() | F.col("sog").between(0.0, MAX_REPORTED_SOG_KNOTS)
    plausible_track_speed = F.col("prev_ts").isNull() | (
        (F.col("dt_prev") > 0)
        & (F.col("implied_sog").isNull() | (F.col("implied_sog") <= F.lit(MAX_IMPLIED_SOG_KNOTS)))
    )
    moving_by_sog = F.col("sog") >= F.lit(MIN_MOVING_SOG_KNOTS)
    moving_by_displacement = (F.col("step_km") >= F.lit(MIN_DISPLACEMENT_KM)) & (F.col("dt_prev") <= F.lit(MAX_TRACK_GAP_SECONDS))

    return df.filter(plausible_reported_speed & plausible_track_speed & ~stationary_words & (moving_by_sog | moving_by_displacement))


def prepare_moving_area_points(df: DataFrame) -> DataFrame:
    return remove_noise_and_stationary(add_track_diagnostics(keep_assignment_area(keep_valid_positions(df))))
