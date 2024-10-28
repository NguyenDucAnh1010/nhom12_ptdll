from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import sys
from func.table import Table as tb

# Lấy mã môn học từ tham số dòng lệnh
if len(sys.argv) != 2:
    print("Vui lòng truyền tên môn học như tham số.")
    sys.exit(1)

subject_name = sys.argv[1]

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("classifystudent") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container") \
    .set("spark.cassandra.input.split.sizeInMB", "64")  # Sửa tham số này

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
    .options(table="subject", keyspace="nhom12", pushdown="false") \
    .load()

# Kết hợp dữ liệu bảng Grade với bảng Student và Subject
joined_df = grades_df \
    .join(students_df, grades_df.idstudent == students_df.idstudent, how="inner") \
    .join(subjects_df, grades_df.idsubject == subjects_df.idsubject, how="inner") \
    .select(grades_df.idstudent, students_df.namestudent, grades_df.idsubject, subjects_df.namesubject, grades_df.grade)

# Lọc theo tên môn học được truyền vào
filtered_df = joined_df.filter(F.col("namesubject") == subject_name)

# Tính điểm trung bình theo môn học
avg_grades_df = filtered_df.groupBy("idsubject", "namesubject") \
    .agg(F.avg("grade").alias("average_grade"))

# Kết hợp điểm trung bình vào DataFrame gốc
joined_with_avg_df = filtered_df \
    .join(avg_grades_df, ["idsubject", "namesubject"], "inner")

# Phân loại điểm của sinh viên dựa trên điểm trung bình
classification_df = joined_with_avg_df.withColumn(
    "classification",
    F.when(F.col("grade") >= F.col("average_grade"), "Giỏi")
    .otherwise("Kém")
)

# Hiển thị kết quả cho tất cả sinh viên
all_students = classification_df.collect()
schema = classification_df.schema
table = tb.create(result=all_students,schema=schema)
for row in table:
    print(row)
# Đóng Spark session
spark.stop()