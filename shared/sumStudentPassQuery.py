from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

# Lấy tham số `term` từ dòng lệnh và chuyển thành int
term = int(sys.argv[1])

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("truyvan") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

# Tạo Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Đọc dữ liệu từ bảng Grade
grades_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load()

# Đọc dữ liệu từ bảng Student
students_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()

# Lọc ra các môn mà sinh viên đã qua (Điều kiện: điểm > 4) và theo kỳ `term`
passed_subjects_df = grades_df.filter((grades_df.grade > 4) & (grades_df.term == term))

# Kết hợp bảng Grade và bảng Student để lấy tên sinh viên
joined_df = passed_subjects_df.join(
    students_df, 
    passed_subjects_df.idstudent == students_df.idstudent, 
    "inner"
).select(students_df.idstudent, students_df.namestudent.alias("namestudent"), passed_subjects_df.term, passed_subjects_df.idsubject)

# Tính tổng số môn đã qua của từng sinh viên theo từng kỳ
total_passed_subjects_df = joined_df \
    .groupBy("idstudent", "namestudent", "term") \
    .agg(F.count("idsubject").alias("total_passed_subjects"))

# Hiển thị kết quả
total_passed_subjects = total_passed_subjects_df.collect()

for row in total_passed_subjects:
    print(f"ID sinh viên: {row['idstudent']}, Tên sinh viên: {row['namestudent']}, Kỳ: {row['term']}, Tổng số môn đã qua: {row['total_passed_subjects']}")
