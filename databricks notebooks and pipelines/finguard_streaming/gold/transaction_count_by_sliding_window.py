from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="finguard.gold.transaction_count_by_sliding_window",
    comment="Count of transactions by sliding window",
)
def transaction_count_by_minute() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions").withColumn(
        "transaction_timestamp", F.to_timestamp("transaction_timestamp")
    )
    transactions_with_watermark = transactions.withWatermark(
        "transaction_timestamp", "5 minutes"
    )
    transaction_count_df = (
        transactions_with_watermark.groupBy(
            F.window("transaction_timestamp", "5 minutes", "1 minute")
        )
        .agg(F.count("*").alias("transaction_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("transaction_count"),
        )
    )
    return transaction_count_df