from pyspark.sql import SparkSession
from pyspark.sql.functions import (explode, arrays_zip, col, to_date,
                                   monotonically_increasing_id, year,
                                   month, dayofmonth, hour, dayofweek,
                                   to_timestamp)


def create_fact_weather_daily(spark: SparkSession, bronze_path: str,
                              silver_path: str) -> None:
    """
    Updates the dim_location, dim_date and fact_weather_daily tables.
    :param spark: Entry point to Spark application
    :param bronze_path: Path to the raw data
    :param silver_path: path to write the silver tables
    """

    df = spark.read.json(bronze_path)

    df = df.select(
        "latitude",
        "longitude",
        "timezone",
        "timezone_abbreviation",
        "utc_offset_seconds",
        "elevation",
        explode(
            arrays_zip(
                "daily.time",
                "daily.weather_code",
                "daily.temperature_2m_max",
                "daily.temperature_2m_min",
                "daily.temperature_2m_mean",
                "daily.apparent_temperature_max",
                "daily.apparent_temperature_min",
                "daily.apparent_temperature_mean",
                "daily.precipitation_sum",
                "daily.rain_sum",
                "daily.snowfall_sum",
                "daily.precipitation_hours",
                "daily.wind_speed_10m_max",
                "daily.wind_gusts_10m_max",
                "daily.wind_direction_10m_dominant",
                "daily.et0_fao_evapotranspiration",
                "daily.shortwave_radiation_sum"
            )
        ).alias("daily")
    )

    df_daily = df.select(
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("timezone_abbreviation"),
        col("utc_offset_seconds"),
        col("elevation"),
        to_date(col("daily.time")).alias("date"),
        col("daily.weather_code"),
        col("daily.temperature_2m_max"),
        col("daily.temperature_2m_min"),
        col("daily.temperature_2m_mean"),
        col("daily.apparent_temperature_max"),
        col("daily.apparent_temperature_min"),
        col("daily.apparent_temperature_mean"),
        col("daily.precipitation_sum"),
        col("daily.rain_sum"),
        col("daily.snowfall_sum"),
        col("daily.precipitation_hours"),
        col("daily.wind_speed_10m_max"),
        col("daily.wind_gusts_10m_max"),
        col("daily.wind_direction_10m_dominant"),
        col("daily.et0_fao_evapotranspiration"),
        col("daily.shortwave_radiation_sum")
    )

    # dim_location
    dim_location = (
        df_daily
        .select(
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds",
            "elevation"
        )
        .distinct()
        .withColumn("location_id", monotonically_increasing_id())
    )

    dim_location.write.mode("overwrite").parquet(
        f"{silver_path}/dim_location"
    )

    # dim_date
    dim_date = (
        df_daily
        .withColumn("date_id", monotonically_increasing_id())
        .select("date_id", "date")
        .distinct()
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", dayofweek("date"))
    )

    dim_date.write.mode("overwrite").parquet(
        f"{silver_path}/dim_date"
    )

    # Fact Table

    fact = (
        df_daily
        .join(
            dim_location,
            ["latitude", "longitude", "timezone", "timezone_abbreviation",
             "utc_offset_seconds", "elevation"],
            "left"
        )
        .join(
            dim_date,
            ["date"],
            "left"
        )
        .select(
            "location_id",
            "date_id",
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "apparent_temperature_mean",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
            "et0_fao_evapotranspiration",
            "shortwave_radiation_sum",
            "year",
            "month"
        )
    )

    fact.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{silver_path}/fact_weather_daily")


def create_fact_weather_hourly(spark: SparkSession, bronze_path: str,
                               silver_path: str) -> None:
    """
    Updates the dim_location, dim_datetime and fact_weather_hourly tables.
    :param spark: Entry point to Spark application
    :param bronze_path: Path to the raw data
    :param silver_path: path to write the silver tables
    """

    df = spark.read.json(bronze_path)

    df = df.select(
        "latitude",
        "longitude",
        "timezone",
        "timezone_abbreviation",
        "utc_offset_seconds",
        "elevation",
        col('hourly_units.*'),
        explode(
            arrays_zip(
                "hourly.time",
                "hourly.temperature_2m",
                "hourly.relative_humidity_2m",
                "hourly.dew_point_2m",
                "hourly.apparent_temperature"
            )
        ).alias("hourly")
    )

    df_hourly = df.select(
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("timezone_abbreviation"),
        col("utc_offset_seconds"),
        col("elevation"),
        to_timestamp(col("hourly.time")).alias("datetime"),
        col("hourly.temperature_2m"),
        col("hourly.relative_humidity_2m"),
        col("hourly.dew_point_2m"),
        col("hourly.apparent_temperature")
    )

    # dim_location
    dim_location = (
        df_hourly
        .select(
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds",
            "elevation"
        )
        .distinct()
        .withColumn("location_id", monotonically_increasing_id())
    )

    # dim_location.write.mode("overwrite").parquet(
    #     f"{silver_path}/dim_location"
    # )

    # dim_datetime
    dim_datetime = (
        df_hourly
        .withColumn("datetime_id", monotonically_increasing_id())
        .select("datetime_id", "datetime")
        .distinct()
        .withColumn("timestamp", col("datetime"))
        .withColumn("year", year("datetime"))
        .withColumn("month", month("datetime"))
        .withColumn("day", dayofmonth("datetime"))
        .withColumn("hour", hour("datetime"))
        .withColumn("day_of_week", dayofweek("datetime"))
    )

    dim_datetime.write.mode("overwrite").parquet(
        f"{silver_path}/dim_datetime"
    )

    # Fact Table

    fact = (
        df_hourly
        .join(
            dim_location,
            ["latitude", "longitude", "timezone", "timezone_abbreviation",
             "utc_offset_seconds", "elevation"],
            "left"
        )
        .join(
            dim_datetime,
            ["datetime"],
            "left"
        )
        .select(
            "location_id",
            "datetime_id",
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "year",
            "month"
        )
    )

    fact.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{silver_path}/fact_weather_hourly")
