from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser(description='Process some inputs for Spark job.')
parser.add_argument('--selected_class', required=True, help='Path to the input CSV file')
args = parser.parse_args()

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("GPA Calculation") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

selected_class = args.selected_class
# Đọc dữ liệu từ bảng student và grade từ Cassandra
student_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load() \
    .filter("idclass = '{}'".format(selected_class))

grade_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load() \
    .join(student_df, "idstudent")


# Tính GPA: trung bình điểm (grade) của từng sinh viên
gpa_df = grade_df.groupBy("idstudent") \
    .agg(F.avg("grade").alias("gpa"))

# Liên kết sinh viên với lớp và GPA
student_gpa_df = student_df.join(gpa_df, "idstudent") \
    .select("idstudent", "namestudent", "idclass", "gpa")

# Hiển thị kết quả
student_gpa_df = student_gpa_df.withColumn("gpa", student_gpa_df["gpa"].cast("string"))

student_gpa_df.cache()
result = student_gpa_df.collect()
for row in result:
    print(row)# result.show()
# student_gpa_df.write.mode("overwrite").parquet("\opt\shared\dataset\gpa_for_class.parquet")
# student_gpa_df.write \
#     .format("org.apache.spark.sql.cassandra") \
#     .mode("append") \
#     .options(table="student_gpa", keyspace="nhom12") \
#     .save()

# Đóng Spark session
spark.stop()
