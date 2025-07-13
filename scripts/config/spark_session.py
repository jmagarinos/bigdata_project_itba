from pyspark.sql import SparkSession

def get_spark_session(app_name="BigDataOnboardingFintech"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .getOrCreate()
    return spark
