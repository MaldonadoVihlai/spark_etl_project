from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, round, avg, sum, stddev, row_number)
from pyspark.sql.window import Window


def create_gold_weather_climate_trends(spark: SparkSession,
                                       daily_silver_path: str,
                                       gold_path: str) -> None:
    """
    Creates the gold_weather_climate_trends table.
    :param spark: Entry point to Spark application
    :param daily_silver_path: Path to the daily silver data
    :param gold_path: path to write the gold table
    """

    daily_data_df = spark.read.parquet(daily_silver_path)
    df = daily_data_df\
        .groupBy('location_id', 'year', 'month')\
        .agg(round(avg('temperature_2m_mean'), 2).alias('yearly_avg_temp'),
             round(sum('rain_sum'), 2).alias('yearly_total_rain'),
             round(sum('snowfall_sum'), 2).alias('yearly_total_snow'),
             round(avg('wind_speed_10m_max'), 2).alias('yearly_avg_wind'),
             round(stddev('precipitation_sum'), 2)
             .alias('rainfall_variability'),
             round(stddev('temperature_2m_mean'), 2)
             .alias('temperature_volatility'))

    window_hottest_month = Window().partitionBy('month')\
        .orderBy(col('monthly_avg_temp').asc())
    window_coldest_month = Window().partitionBy('month')\
        .orderBy(col('monthly_avg_temp').desc())

    monthly_avg = (
        daily_data_df.groupBy('location_id', 'year', 'month')
        .agg(avg('temperature_2m_mean').alias('monthly_avg_temp'))
    )

    df_hottest_month = (monthly_avg
                        .select('location_id', 'year', 'month',
                                row_number().over(window=window_hottest_month)
                                .alias('rw_num'))
                        .filter(col('rw_num') == 1).drop('rw_num')
                        )
    df_coldest_month = (monthly_avg
                        .select('location_id', 'year', 'month',
                                row_number().over(window=window_coldest_month)
                                .alias('rw_num'))
                        .filter(col('rw_num') == 1).drop('rw_num')
                        )

    gold_weather_climate_trends = df.join(df_hottest_month, ['location_id',
                                                             'year',
                                                             'month'], 'left')\
        .join(df_coldest_month, ['location_id', 'year', 'month'], 'left')\
        .select('location_id', 'yearly_avg_temp', 'yearly_total_rain',
                'yearly_total_snow', 'yearly_avg_wind', 'rainfall_variability',
                'temperature_volatility', 'year', 'month')

    gold_weather_climate_trends.write \
        .mode("overwrite") \
        .partitionBy("location_id") \
        .parquet(f"{gold_path}/gold_weather_climate_trends")


def create_gold_weather_hourly_patterns(spark: SparkSession,
                                        hourly_silver_path: str,
                                        dim_datetime_path: str,
                                        gold_path: str) -> None:
    """
    Creates the gold_weather_hourly_patterns table.
    :param spark: Entry point to Spark application
    :param hourly_silver_path: Path to the hourly silver data
    :param dim_datetime_path: Path to the dim datetime table
    :param gold_path: path to write the gold table
    """

    dim_datetime = spark.read.parquet(dim_datetime_path).select('datetime_id',
                                                                'hour')
    gold_weather_hourly_patterns = spark.read.parquet(hourly_silver_path)\
        .join(dim_datetime, 'datetime_id', 'left') \
        .groupBy('location_id', 'year', 'month', 'hour') \
        .agg(avg('temperature_2m').alias('avg_temp_by_hour'),
             avg('relative_humidity_2m').alias('avg_humidity_by_hour'))\
        .select('location_id', 'hour', 'avg_temp_by_hour',
                'avg_humidity_by_hour', 'year', 'month')

    gold_weather_hourly_patterns.write \
        .mode("overwrite") \
        .partitionBy("location_id") \
        .parquet(f"{gold_path}/gold_weather_hourly_patterns")
