from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.config import LOCAL_RAW_PATH, PROCESSED_OUTPUT, CURATED_OUTPUT
from src.utils.spark_session import create_spark_session


def run_transformation(input_path: str | None = None) -> None:
    """
    Reads the raw NASA NeoWs JSON, converts near_earth_objects to a map,
    flattens nested fields, creates analytical columns, and saves
    processed + curated datasets in Parquet.
    """
    spark = create_spark_session()

    raw_input = str(input_path or LOCAL_RAW_PATH)

    df = spark.read.option("multiline", "true").json(raw_input)

    close_approach_schema = ArrayType(
        StructType([
            StructField("close_approach_date", StringType(), True),
            StructField("close_approach_date_full", StringType(), True),
            StructField("epoch_date_close_approach", LongType(), True),
            StructField(
                "relative_velocity",
                StructType([
                    StructField("kilometers_per_second", StringType(), True),
                    StructField("kilometers_per_hour", StringType(), True),
                    StructField("miles_per_hour", StringType(), True),
                ]),
                True,
            ),
            StructField(
                "miss_distance",
                StructType([
                    StructField("astronomical", StringType(), True),
                    StructField("lunar", StringType(), True),
                    StructField("kilometers", StringType(), True),
                    StructField("miles", StringType(), True),
                ]),
                True,
            ),
            StructField("orbiting_body", StringType(), True),
        ])
    )

    asteroid_schema = StructType([
        StructField("id", StringType(), True),
        StructField("neo_reference_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nasa_jpl_url", StringType(), True),
        StructField("absolute_magnitude_h", DoubleType(), True),
        StructField(
            "estimated_diameter",
            StructType([
                StructField(
                    "meters",
                    StructType([
                        StructField("estimated_diameter_min", DoubleType(), True),
                        StructField("estimated_diameter_max", DoubleType(), True),
                    ]),
                    True,
                )
            ]),
            True,
        ),
        StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
        StructField("is_sentry_object", BooleanType(), True),
        StructField("close_approach_data", close_approach_schema, True),
    ])

    neo_map_schema = MapType(StringType(), ArrayType(asteroid_schema))

    neo_map_df = df.select(
        F.from_json(
            F.to_json(F.col("near_earth_objects")),
            neo_map_schema
        ).alias("near_earth_objects_map")
    )

    exploded_dates_df = neo_map_df.select(
        F.explode("near_earth_objects_map").alias("approach_date", "asteroids")
    )

    exploded_asteroids_df = exploded_dates_df.select(
        "approach_date",
        F.explode("asteroids").alias("asteroid"),
    )

    flattened_df = exploded_asteroids_df.select(
        F.col("approach_date"),
        F.col("asteroid.id").alias("id"),
        F.col("asteroid.neo_reference_id").alias("neo_reference_id"),
        F.col("asteroid.name").alias("name"),
        F.col("asteroid.nasa_jpl_url").alias("nasa_jpl_url"),
        F.col("asteroid.absolute_magnitude_h").alias("absolute_magnitude_h"),
        F.col("asteroid.estimated_diameter.meters.estimated_diameter_min").alias("diameter_min_meters"),
        F.col("asteroid.estimated_diameter.meters.estimated_diameter_max").alias("diameter_max_meters"),
        F.col("asteroid.is_potentially_hazardous_asteroid").alias("is_potentially_hazardous_asteroid"),
        F.col("asteroid.is_sentry_object").alias("is_sentry_object"),
        F.col("asteroid.close_approach_data")[0]["close_approach_date"].alias("close_approach_date"),
        F.col("asteroid.close_approach_data")[0]["close_approach_date_full"].alias("close_approach_date_full"),
        F.col("asteroid.close_approach_data")[0]["epoch_date_close_approach"].alias("epoch_date_close_approach"),
        F.col("asteroid.close_approach_data")[0]["relative_velocity"]["kilometers_per_hour"].cast("double").alias("relative_velocity_kph"),
        F.col("asteroid.close_approach_data")[0]["relative_velocity"]["kilometers_per_second"].cast("double").alias("relative_velocity_kps"),
        F.col("asteroid.close_approach_data")[0]["miss_distance"]["kilometers"].cast("double").alias("miss_distance_km"),
        F.col("asteroid.close_approach_data")[0]["miss_distance"]["lunar"].cast("double").alias("miss_distance_lunar"),
        F.col("asteroid.close_approach_data")[0]["orbiting_body"].alias("orbiting_body"),
    )

    final_df = (
        flattened_df
        .withColumn("approach_date", F.to_date("approach_date"))
        .withColumn("close_approach_date", F.to_date("close_approach_date"))
        .withColumn("year", F.year("approach_date"))
        .withColumn("month", F.month("approach_date"))
        .withColumn(
            "diameter_avg_meters",
            (F.col("diameter_min_meters") + F.col("diameter_max_meters")) / 2,
        )
    )

    curated_df = (
        final_df.groupBy("year", "month")
        .agg(
            F.count("*").alias("total_asteroids"),
            F.sum(
                F.when(F.col("is_potentially_hazardous_asteroid") == True, 1).otherwise(0)
            ).alias("total_hazardous"),
            F.avg("relative_velocity_kph").alias("avg_velocity_kph"),
            F.avg("miss_distance_km").alias("avg_miss_distance_km"),
            F.avg("diameter_avg_meters").alias("avg_diameter_meters"),
        )
    )

    PROCESSED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    CURATED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    final_df.write.mode("overwrite").parquet(str(PROCESSED_OUTPUT))
    curated_df.write.mode("overwrite").parquet(str(CURATED_OUTPUT))

    print(f"Raw input used: {raw_input}")
    print(f"Processed dataset saved to: {PROCESSED_OUTPUT}")
    print(f"Curated dataset saved to: {CURATED_OUTPUT}")

    spark.stop()


if __name__ == "__main__":
    run_transformation()