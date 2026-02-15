from pyspark.sql import SparkSession, DataFrame
from lxml import etree
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

pjanind3_schema = StructType(
        [
            StructField("geo", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("indic_de", StringType(), True),
            StructField("freq", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("value", DoubleType(), True),
        ]
    )


def parse_eurostat_series(spark: SparkSession, path: str) -> DataFrame:
    """
    Parse Eurostat XML file using RDDs and return a Spark DataFrame.
    :param spark: Entry point to Spark application
    :param path: Path to the XML data
    :return: DataFrame
    """
    sc = spark.sparkContext
    rdd = sc.wholeTextFiles(path)

    def parse_partition(records):

        for _, xml_content in records:
            try:
                root = etree.fromstring(xml_content.encode("utf-8"))

                namespace = root.nsmap.get(None) or root.nsmap.get("g")
                ns = {"g": namespace}

                for series in root.findall(".//g:Series", namespaces=ns):

                    series_key = {
                        v.get("id"): v.get("value")
                        for v in series.findall(".//g:SeriesKey/g:Value",
                                                namespaces=ns)
                    }

                    for obs in series.findall(".//g:Obs", namespaces=ns):

                        year_elem = obs.find(".//g:ObsDimension",
                                             namespaces=ns)
                        value_elem = obs.find(".//g:ObsValue", namespaces=ns)

                        if year_elem is None or value_elem is None:
                            continue

                        value_raw = value_elem.get("value")
                        if value_raw is None:
                            continue

                        yield {
                            **series_key,
                            "year": int(year_elem.get("value")),
                            "value": float(value_raw),
                        }
            except Exception as e:
                print("XML parse error:", e)

    parsed_rdd = rdd.mapPartitions(parse_partition)
    df = spark.createDataFrame(parsed_rdd, schema=pjanind3_schema)

    return df


def write_eurostat_data(spark: SparkSession, data_df: DataFrame,
                        silver_table_path: str) -> None:
    data_df.write.mode("overwrite").parquet(
        f"{silver_table_path}"
        )
