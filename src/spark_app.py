import os
from pyspark.sql import SparkSession


def make_spark() -> SparkSession:
    memory = os.getenv("SPARK_DRIVER_MEMORY", "6g")
    return (
        SparkSession.builder
        .appName("bornholm-ais-collision-search")
        .master("local[*]")
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "360"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
