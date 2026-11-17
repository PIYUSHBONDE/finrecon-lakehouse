"""A bounded local Spark runtime shared by ingestion and its verification."""

import os


def spark_session(name, configs=None):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.sql.warehouse.dir", "/lakehouse/warehouse")
        .config("spark.jars.ivy", os.environ.get("SPARK_IVY_DIR", "/tmp/finrecon-ivy"))
    )
    for key, value in (configs or {}).items():
        builder = builder.config(key, value)
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"],
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
