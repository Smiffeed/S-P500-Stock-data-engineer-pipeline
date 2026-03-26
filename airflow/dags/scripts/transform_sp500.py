import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, countDistinct, when, expr


def parse_args():
    parser = argparse.ArgumentParser(description="Transform SP500 raw files into mart parquet")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--raw-prefix", default="raw", help="Raw folder prefix in bucket")
    parser.add_argument(
        "--output-prefix",
        default="processed/sp500_sector_daily",
        help="Output folder prefix in bucket",
    )
    return parser.parse_args()


args = parse_args()

spark = SparkSession.builder.appName("sp500-transform").getOrCreate()

stocks_path = f"gs://{args.bucket}/{args.raw_prefix}/*/sp500_stocks.csv"
companies_path = f"gs://{args.bucket}/{args.raw_prefix}/*/sp500_companies.csv"
output_path = f"gs://{args.bucket}/{args.output_prefix}"

stocks = (
    spark.read.option("header", True).csv(stocks_path)
    .select(
        to_date(col("Date")).alias("date"),
        col("Symbol").alias("symbol"),
        col("Close").cast("double").alias("close"),
        col("Volume").cast("double").alias("volume"),
    )
)

stocks = stocks.filter(col("date").isNotNull() & col("symbol").isNotNull())

companies = (
    spark.read.option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")
    .csv(companies_path)
    .select(
        col("Symbol").alias("symbol"),
        col("Sector").alias("sector"),
        expr("try_cast(trim(Weight) as double)").alias("weight"),
    )
)

companies = companies.filter(
    col("symbol").isNotNull() & col("sector").isNotNull() & col("weight").isNotNull()
)

joined = stocks.join(companies, on="symbol", how="inner").filter(col("close").isNotNull())

sector_mart = (
    joined
    .groupBy("date", "sector")
    .agg(
        _sum(col("close") * col("weight")).alias("weighted_close_sum"),
        _sum("weight").alias("weight_sector"),
        _sum("volume").alias("total_volume"),
        countDistinct("symbol").alias("company_count"),
    )
    .withColumn(
        "avg_close",
        when(col("weight_sector") > 0, col("weighted_close_sum") / col("weight_sector")),
    )
)

sp500_daily = (
    joined.groupBy("date")
    .agg(
        (_sum(col("close") * col("weight")) / _sum("weight")).alias("sp500_avg_close")
    )
)

mart = (
    sector_mart.join(sp500_daily, on="date", how="left")
    .select(
        "date",
        "sector",
        "avg_close",
        "sp500_avg_close",
        "total_volume",
        "company_count",
        "weight_sector",
    )
)

mart.write.mode("overwrite").parquet(output_path)

spark.stop()