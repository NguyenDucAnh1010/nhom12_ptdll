from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys

# Nhận tham số dòng lệnh (có thể nhận từ tham số truyền vào từ ngoài)
# term = int(sys.argv[1]) if len(sys.argv) > 1 else None
name_department = sys.argv[1] if len(sys.argv) > 1 else None
name_class = sys.argv[2] if len(sys.argv) > 2 else None

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("FaildStudent") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

# Tạo Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
# Thiết lập mức log thành "ERROR" để chỉ hiển thị lỗi nghiêm trọng
spark.sparkContext.setLogLevel("OFF")

## Đọc dữ liệu từ các bảng trong Cassandra
grades_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load()

students_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()

classes_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="class", keyspace="nhom12") \
    .load()

departments_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="department", keyspace="nhom12") \
    .load()

subjects_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="subject", keyspace="nhom12") \
    .load()

# Thực hiện join giữa các bảng với alias
result_df = grades_df.alias("g") \
    .join(students_df.alias("s"), "idstudent") \
    .join(classes_df.alias("c"), "idclass") \
    .join(departments_df.alias("d"), "iddepartment") \
    .join(subjects_df.alias("sub"), "idsubject")

## Lưu DataFrame vào thư mục shared dưới dạng Parquet để sử dụng lại
result_df.write.mode("overwrite").parquet("/opt/shared/failed_students_data.parquet")

#Doc ket qua da luu
#result_df = spark.read.parquet("/opt/shared/failed_students_data.parquet")


## Áp dụng điều kiện lọc dựa trên giá trị của name_class và name_department
if name_class:
    result_df = result_df.filter((result_df["namedepartment"] == name_department) & (result_df["nameclass"] == name_class))
else:
    result_df = result_df.filter(result_df["namedepartment"] == name_department)

result_df = result_df.select("namedepartment", "namesubject", "nameclass", "term", "idstudent", "namestudent", "grade")

# Lọc các sinh viên có điểm nhỏ hơn 4
result_below_4_df = result_df.filter(result_df["grade"] < 4)

## Lưu DataFrame vào thư mục shared dưới dạng Parquet để sử dụng lại
result_below_4_df.write.mode("overwrite").parquet("/opt/shared/failed_students.parquet")

# Thu thập và hiển thị kết quả
result = result_below_4_df.collect()

for row in result:
    diem = str(round(float(row["grade"]), 2))
    print(f"Khoa: {row['namedepartment']}, Lớp: {row['nameclass']}, Môn: {row['namesubject']}, Kỳ: {row['term']}, Mã sinh viên: {row['idstudent']}, Tên sinh viên: {row['namestudent']}, Điểm: {diem}")

# Đóng Spark session
spark.stop()
