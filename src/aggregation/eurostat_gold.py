from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, create_map, first


def create_gold_demographics_indicators(spark: SparkSession,
                                        silver_path: str,
                                        gold_path: str) -> None:
    """
    Creates the gold_demographics_indicators table.
    :param spark: Entry point to Spark application
    :param silver_path: Path to the silver data
    :param gold_path: Path to write the gold table
    """
    indicator_map = {
        "MEDAGEPOP": "median_age_total",
        "MMEDAGEPOP": "median_age_male",
        "FMEDAGEPOP": "median_age_female",
        "DEPRATIO1": "dependency_ratio"
    }

    data_df = spark.read.parquet(silver_path)
    mapping_expr = create_map(
        *[lit(x) for kv in indicator_map.items() for x in kv]
    )

    df_enriched = data_df.withColumn(
        "indicator_name",
        mapping_expr[col("indic_de")]
    )

    gold_df = (
        df_enriched
        .groupBy("geo", "year")
        .pivot("indicator_name")
        .agg(first("value"))
    )

    gold_df.write.mode("overwrite") \
        .parquet(f"{gold_path}/gold_demographics_indicators")
