from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session


def run_transformation(input_path: str) -> None:
    spark = create_spark_session()

    df = spark.read.option("multiline", "true").json(input_path)

    df.printSchema()
    df.show(3, truncate=False)

    spark.stop()