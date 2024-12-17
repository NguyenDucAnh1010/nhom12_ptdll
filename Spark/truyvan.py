from pyspark.sql import SparkSession
from pyspark import SparkConf

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

# Đọc dữ liệu từ bảng grade trong Cassandra sử dụng RDD
grades_rdd = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load() \
    .rdd  # Chuyển DataFrame sang RDD để có thể sử dụng MapReduce

# MapReduce để lọc sinh viên có điểm nhỏ hơn 4
students_below_4_rdd = grades_rdd \
    .map(lambda row: (row['idstudent'], row['namestudent'], row['idsubject'], row['namesubject'], row['grade'])) \
    .filter(lambda student: student[4] < 4)  # Lọc các sinh viên có điểm nhỏ hơn 4

# Thu thập và hiển thị kết quả
students_below_4 = students_below_4_rdd.collect()

for student in students_below_4:
    print(f"ID sinh viên: {student[0]}, Tên sinh viên: {student[1]}, Mã môn học: {student[2]}, Tên môn học: {student[3]}, Điểm: {student[4]}")

# Đóng Spark session
spark.stop()
