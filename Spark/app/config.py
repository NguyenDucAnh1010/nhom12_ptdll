from pyspark.sql import SparkSession
from pyspark import SparkConf


def conf():
    # Cấu hình Spark
    conf = SparkConf() \
        .setAppName("FaildStudents") \
        .setMaster("spark://spark-master:7077") \
        .set("spark.executor.heartbeatInterval", "100s") \
        .set("spark.network.timeout", "600s") \
        .set("spark.rpc.message.maxSize", "1024") \
        .set("spark.cassandra.connection.host", "cassandra-container")

    # Tạo Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark