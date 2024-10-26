from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

# Lấy mã môn học từ tham số dòng lệnh
if len(sys.argv) != 2:
    print("Vui lòng truyền tên môn học như tham số.")
    sys.exit(1)

subject_name = sys.argv[1]

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

# Đọc dữ liệu từ bảng grade trong Cassandra
grades_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load()

students_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()

subjects_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="subject", keyspace="nhom12") \
    .load()

# Kết hợp dữ liệu bảng Grade với bảng Student và Subject
joined_df = grades_df \
    .join(students_df, grades_df.idstudent == students_df.idstudent, how="inner") \
    .join(subjects_df, grades_df.idsubject == subjects_df.idsubject, how="inner") \
    .select(grades_df.idstudent, students_df.namestudent, grades_df.idsubject, subjects_df.namesubject, grades_df.grade)

# Tạo cột xếp hạng cho từng sinh viên theo môn học
ranked_df = joined_df \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("idsubject").orderBy(F.desc("grade"))
    ))

# Lọc ra Top 10 sinh viên theo mã môn học
top_10_students_df = ranked_df \
    .filter(ranked_df["rank"] <= 10) \
    .filter(ranked_df["namesubject"] == subject_name) \
    .select("idstudent", "namestudent", "idsubject", "namesubject", "grade")

# Hiển thị kết quả
top_10_students = top_10_students_df.collect()

for student in top_10_students:
    diem = str(round(float(student["grade"]), 2))
    print(f"ID sinh viên: {student['idstudent']}, Tên sinh viên: {student['namestudent']}, Mã môn học: {student['idsubject']}, Tên môn học: {student['namesubject']}, Điểm: {diem}")

# Đóng Spark session
spark.stop()
