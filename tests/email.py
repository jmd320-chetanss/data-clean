from src.cleaners.EmailCleaner import EmailCleaner
from pyspark.sql import SparkSession


def test_email_cleaner():

    spark = SparkSession.builder \
        .appName("EmailCleanerTest") \
        .getOrCreate()

    email_number_df = spark.read.csv(
        "tests/emails.csv", header=True).limit(20)

    email_cleaner = EmailCleaner(
        parse_count=1,
    )

    result_df = email_number_df.withColumn(
        "email_cleaned",
        email_cleaner.clean_col("email")
    )

    result_df.show(truncate=False)


test_email_cleaner()
