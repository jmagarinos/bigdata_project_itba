from pyspark.sql import SparkSession

def get_spark_session(app_name="BigDataOnboardingFintech"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark
