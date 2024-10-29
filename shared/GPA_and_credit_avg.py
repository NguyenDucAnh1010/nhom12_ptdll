from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import argparse
from func.table import Table as tb

# parser = argparse.ArgumentParser(description='Process some inputs for Spark job.')
# parser.add_argument('--selected_class', required=True, help='Path to the input CSV file')
# args = parser.parse_args()

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("GPA_Credit Calculation") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# selected_class = args.selected_class

class_df = spark.read \
            .format("org.apache.spark.sql.cassandra")\
            .options(table="class", keyspace="nhom12") \
            .load()\
            # .filter("nameclass='{}'".format(selected_class))
# Đọc dữ liệu từ bảng student và grade từ Cassandra
student_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load() \
    .join(class_df,"idclass")

grade_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load() \
    .join(student_df, "idstudent")

subject_df = spark.read\
    .format("org.apache.spark.sql.cassandra") \
    .options(table="subject", keyspace="nhom12") \
    .load() \
    .join(grade_df, "idsubject")\

# Tính GPA: trung bình điểm (grade) của từng sinh viên
gpa_credit_df = subject_df.groupBy("term") \
    .agg(
        F.avg("grade").alias("gpa"),
        # F.sum("credit").alias("sum_credits"),
        F.avg("credit").alias("avg_credits")
    )\
    .orderBy("term")
gpa_credit_df= gpa_credit_df.withColumn("Rate", (F.col("gpa"))/(F.col("avg_credits")))\
                .select(F.col("term").alias("Kỳ học"),F.col("gpa").alias("GPA"),F.col("avg_credits").alias("Trung bình tín chỉ của một môn"),F.col("Rate").alias("Tỉ lệ đánh giá"))

# Hiển thị kết quả
# student_gpa_df = student_gpa_df.withColumn("gpa", round(student_gpa_df["gpa"], 2))

gpa_credit_df.cache()
result = gpa_credit_df.collect()
schema = gpa_credit_df.schema

table = tb.create(result=result,schema=schema)
for row in table:
    print(row)
# for row in result:
#     print(row)# result.show()
# student_gpa_df.write.mode("overwrite").parquet("\opt\shared\dataset\gpa_for_class.parquet")
# student_gpa_df.write \
#     .format("org.apache.spark.sql.cassandra") \
#     .mode("append") \
#     .options(table="student_gpa", keyspace="nhom12") \
#     .save()

# Đóng Spark session
spark.stop()
