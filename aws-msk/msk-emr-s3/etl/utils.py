from pyspark.sql.functions import col


def clean(dataframe):
    """Drop invalid transactions and bound duplicate-tracking state to one day."""

    return (
        dataframe
        .withWatermark("timestamp", "1 day")
        .dropDuplicates(["transaction_id"])
        .filter(col("amount") > 0)
    )