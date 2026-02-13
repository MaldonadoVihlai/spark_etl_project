from common.spark_session import get_spark
from common.config_loader import load_config
from ingestion.open_meteo import OpenMeteoClient
from raw.json_saver import save_raw_json
from processing.open_meteo_json_parser import (create_fact_weather_daily,
                                               create_fact_weather_hourly)
from datetime import date


def main(config_path: str):
    config = load_config(config_path)
    spark = get_spark("open_meteo_etl")
    print(config)
    time_data_retrieval = config["filters"]["time"]

    client = OpenMeteoClient()
    for year in time_data_retrieval:
        today = date.today()
        current_year = today.year
        if year == current_year:
            end_date = today
        else:
            end_date = date(year, 12, 31)
        data = client.fetch_archive(
            start_date=f"{year}-01-01",
            end_date=end_date,
            daily=["weather_code", "temperature_2m_max", "temperature_2m_mean",
                   "apparent_temperature_mean",
                   "temperature_2m_min", "apparent_temperature_max",
                   "apparent_temperature_min", "precipitation_sum",
                   "rain_sum", "snowfall_sum", "precipitation_hours",
                   "wind_speed_10m_max", "wind_gusts_10m_max",
                   "wind_direction_10m_dominant",
                   "et0_fao_evapotranspiration", "shortwave_radiation_sum"],
            hourly=["temperature_2m", "relative_humidity_2m", "dew_point_2m",
                    "apparent_temperature"],
            timezone="America/Bogota"
        )
        raw_path = f'{config["storage"]["raw_path"]}/bogota_meteo_data_{year}.json'
        save_raw_json(str(data), raw_path)

    raw_path = f'{config["storage"]["raw_path"]}/'
    silver_path = f'{config["storage"]["silver_path"]}'

    create_fact_weather_daily(spark, bronze_path=raw_path,
                              silver_path=silver_path)
    create_fact_weather_hourly(spark, bronze_path=raw_path,
                               silver_path=silver_path)


if __name__ == "__main__":
    main('configs/data_ingestion_open_meteo.yaml')
