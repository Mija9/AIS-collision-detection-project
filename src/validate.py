from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.settings import MIN_POINTS_AFTER, MIN_POINTS_BEFORE, MIN_POINTS_IN_WINDOW, TRAJECTORY_WINDOW_MINUTES

WINDOW_SECONDS = TRAJECTORY_WINDOW_MINUTES * 60


def add_candidate_ids(candidates: DataFrame) -> DataFrame:
    w = Window.orderBy(F.col("separation_m").asc(), F.col("time_gap_seconds").asc(), F.col("event_timestamp").asc())
    return candidates.withColumn("candidate_id", F.row_number().over(w))


def choose_supported_event(candidates: DataFrame, moving_df: DataFrame) -> DataFrame:
    numbered = add_candidate_ids(candidates)
    candidate_vessels = numbered.select(
        "candidate_id", "event_timestamp", F.lit("a").alias("side"), F.col("mmsi_a").alias("mmsi")
    ).unionByName(
        numbered.select("candidate_id", "event_timestamp", F.lit("b").alias("side"), F.col("mmsi_b").alias("mmsi"))
    )

    support_points = (
        moving_df.select("mmsi", "timestamp", "latitude", "longitude", "sog")
        .join(F.broadcast(candidate_vessels), "mmsi", "inner")
        .withColumn("event_epoch", F.col("event_timestamp").cast("long"))
        .withColumn("point_epoch", F.col("timestamp").cast("long"))
        .filter(F.col("point_epoch").between(F.col("event_epoch") - WINDOW_SECONDS, F.col("event_epoch") + WINDOW_SECONDS))
    )

    support = support_points.groupBy("candidate_id", "side").agg(
        F.count("*").alias("n_points"),
        F.sum(F.when(F.col("point_epoch") < F.col("event_epoch"), 1).otherwise(0)).alias("n_before"),
        F.sum(F.when(F.col("point_epoch") > F.col("event_epoch"), 1).otherwise(0)).alias("n_after"),
        F.avg("sog").alias("avg_sog_window"),
    )

    a = support.filter("side = 'a'").select(
        "candidate_id", F.col("n_points").alias("points_a"), F.col("n_before").alias("before_a"),
        F.col("n_after").alias("after_a"), F.col("avg_sog_window").alias("avg_sog_a")
    )
    b = support.filter("side = 'b'").select(
        "candidate_id", F.col("n_points").alias("points_b"), F.col("n_before").alias("before_b"),
        F.col("n_after").alias("after_b"), F.col("avg_sog_window").alias("avg_sog_b")
    )

    return (
        numbered.join(a, "candidate_id", "left").join(b, "candidate_id", "left")
        .fillna(0, ["points_a", "before_a", "after_a", "points_b", "before_b", "after_b"])
        .filter(
            (F.col("points_a") >= MIN_POINTS_IN_WINDOW) & (F.col("points_b") >= MIN_POINTS_IN_WINDOW)
            & (F.col("before_a") >= MIN_POINTS_BEFORE) & (F.col("before_b") >= MIN_POINTS_BEFORE)
            & (F.col("after_a") >= MIN_POINTS_AFTER) & (F.col("after_b") >= MIN_POINTS_AFTER)
        )
        .orderBy(F.col("separation_m").asc(), F.col("time_gap_seconds").asc())
        .limit(1)
    )
