from common.spark_session import get_spark
from common.config_loader import load_config
from ingestion.eurostat_api import EurostatFilterClient
from processing.eurostat_xml_parser import (parse_eurostat_series,
                                            write_eurostat_data)
from aggregation.eurostat_gold import create_gold_demographics_indicators


def main(config_path: str):
    config = load_config(config_path)
    spark = get_spark("eurostat_ingestion")
    print(config)
    base_url = config["base_url"]
    time_data = config["filters"]["time"]
    unit_data = config["filters"]["unit"]
    indic_data = config["filters"]["indic_de"]
    freq_data = config["filters"]["freq"]
    raw_path = f'{config["storage"]["raw_path"]}'
    silver_path = f'{config["storage"]["silver_path"]}'
    gold_path = f'{config["storage"]["gold_path"]}'

    client = EurostatFilterClient("demo_r_pjanind3", base_url, raw_path)

    xml_path = client.fetch_xml(
        filters={
            "freq": freq_data,
            "indic_de": indic_data,
            "unit": unit_data,
            "TIME_PERIOD": time_data,
        }
    )
    print(xml_path)
    data_df = parse_eurostat_series(spark, raw_path)
    write_eurostat_data(spark, data_df, silver_path+'/population_structure')

    create_gold_demographics_indicators(spark, silver_path
                                        + '/population_structure',
                                        gold_path)


if __name__ == "__main__":
    main('configs/eurostat_demo_r_pjanind3.yaml')
