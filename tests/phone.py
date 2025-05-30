from src.cleaners.PhoneCleaner import PhoneCleaner
from pyspark.sql import SparkSession


def test_phone_cleaner():

    spark = SparkSession.builder \
        .appName("PhoneCleanerTest") \
        .getOrCreate()

    phone_number_df = spark.read.csv(
        "tests/phone_numbers_old.csv", header=True)

    phone_cleaner = PhoneCleaner()

    result_df = phone_number_df.withColumn(
        "phone_cleaned",
        phone_cleaner.clean_col("phone")
    )

    result_df.show(truncate=False)
    result_df.write.csv("tests/phone_numbers_cleaned.csv", header=True)


test_phone_cleaner()
