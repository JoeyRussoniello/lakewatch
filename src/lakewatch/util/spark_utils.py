"""
spark_utils: Allows referencing SparkSessions locally, while
still supporting remote spark sessions in fabric
"""

from pyspark.sql import SparkSession


def get_spark(create_local: bool = False) -> SparkSession:
    """Lazily gets/creates a spark session"""
    s = SparkSession.getActiveSession()
    if s:
        return s
    if create_local:
        return (
            SparkSession.builder.master("local[*]").appName("lakewatch").getOrCreate()
        )

    raise RuntimeError(
        "No active SparkSession. Run inside Spark or enable create_local."
    )
