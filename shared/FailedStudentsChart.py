from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
from func.table import Table as tb
from pyspark.sql import functions as F


# Nhận tham số dòng lệnh (có thể nhận từ tham số truyền vào từ ngoài)
# term = int(sys.argv[1]) if len(sys.argv) > 1 else None
name_department = sys.argv[1] if len(sys.argv) > 1 else None
name_class = sys.argv[2] if len(sys.argv) > 2 else None

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("FaildStudentChart") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

# Tạo Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#Doc ket qua da luu
# failed_students_data = spark.read.parquet("/opt/shared/failed_students_data.parquet")

# failed_students = spark.read.parquet("/opt/shared/failed_students.parquet")
class_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="class", keyspace="nhom12") \
    .load()
department_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="department", keyspace="nhom12") \
    .load()
grade_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load()
students_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()\
    .join(class_data,"idclass")\
    .join(department_data,"iddepartment")\
    .join(grade_data,"idstudent")
## Áp dụng điều kiện lọc dựa trên giá trị của name_class và name_department
if name_class:
    # Tính toán số sinh viên trượt môn theo từng kỳ cua lop do
    failed_students_by_term = students_data \
        .filter((F.col("nameclass") == name_class) & (F.col("grade") < 4))\
        .groupBy("namedepartment", "nameclass", "term") \
        .count()
    
    # Tổng số sinh viên theo lớp cụ thể đó
    total_students_by_term = students_data \
        .filter(failed_students_by_term["nameclass"] == name_class) \
        .groupBy("namedepartment", "nameclass", "term") \
        .count()
    
    failed_rate_by_term = failed_students_by_term \
        .join(total_students_by_term, ["namedepartment", "nameclass", "term"]) \
        .withColumn("Tỷ lệ trượt", (failed_students_by_term["count"] / total_students_by_term["count"]) * 100) \
        .select("namedepartment", "nameclass", "term", "Tỷ lệ trượt")\
        .orderBy("term")
else:
    # Tính toán số sinh viên trượt môn theo từng kỳ cho tất cả các lớp trong khoa
    failed_students_by_term = students_data \
        .filter((F.col("namedepartment") == name_department) & (F.col("grade") < 4)) \
        .groupBy("namedepartment","term") \
        .count()

    # Tổng số sinh viên theo khoa và từng lớp
    total_students_by_term = students_data \
        .filter((F.col("namedepartment") == name_department)) \
        .groupBy("namedepartment","term") \
        .count()
    
    failed_rate_by_term = failed_students_by_term \
        .join(total_students_by_term, ["namedepartment","term"]) \
        .withColumn("Tỷ lệ trượt", (failed_students_by_term["count"] / total_students_by_term["count"]) * 100) \
        .select("namedepartment", "term", "Tỷ lệ trượt")\
        .orderBy("term")


# failed_students_by_term.show()

# total_students_by_term.show()

# Tính toán tỷ lệ trượt môn theo khoa và kỳ

# failed_rate_by_term.show()
# Thu thập và hiển thị kết quả
result = failed_rate_by_term.collect()
schema = failed_rate_by_term.schema

table = tb.create(result=result,schema=schema)
for row in table:
    print(row)
# Đóng Spark session
spark.stop()
