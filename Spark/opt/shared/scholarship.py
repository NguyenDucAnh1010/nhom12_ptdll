import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from func.table import Table as tb

# Kiểm tra tham số dòng lệnh
if len(sys.argv) != 3:
    print("Vui lòng truyền tên khoa và kỳ học như tham số.")
    sys.exit(1)

# Lấy tên khoa từ tham số dòng lệnh
selected_department_name = sys.argv[1]  # Lấy tên khoa dưới dạng chuỗi

# Kiểm tra kỳ học
try:
    selected_term = int(sys.argv[2])  # Chuyển đổi tham số thành số nguyên
except ValueError:
    print("Kỳ học phải là một số nguyên.")
    sys.exit(1)

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("Scholarship") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

# Tạo Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Đọc dữ liệu từ các bảng Cassandra
grades_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="grade", keyspace="nhom12") \
    .load()

students_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()

# Đọc dữ liệu từ bảng class (lớp)
class_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="class", keyspace="nhom12") \
    .load()

departments_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="department", keyspace="nhom12") \
    .load()

# Đọc dữ liệu từ bảng subjects (môn học)
subjects_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="subject", keyspace="nhom12") \
    .load()

# Lọc ra sinh viên có tất cả các môn đều đạt điểm trên 4 trong kỳ học đã chọn
eligible_grades_df = grades_df.filter(
    (F.col("grade") > 4) & (F.col("term") == selected_term)
)

# Nhóm theo ID sinh viên và kỳ học để tính tổng tín chỉ
total_credit_df = eligible_grades_df.join(subjects_df, "idsubject") \
    .groupBy("idstudent", "term") \
    .agg(
        F.sum("credit").alias("total_credit")
    )

# Lấy danh sách sinh viên có tổng số tín chỉ ít nhất là 12 trong kỳ đã chọn
eligible_students_df = total_credit_df.filter(F.col("total_credit") >= 12)

# Kết hợp với thông tin sinh viên, lớp và khoa
final_df = eligible_students_df.join(students_df, "idstudent") \
    .join(class_df, "idclass") \
    .join(departments_df, "iddepartment") \
    .filter(F.col("namedepartment") == selected_department_name) \
    .select(
        "idstudent",
        "namestudent",
        "nameclass",         
        "namedepartment",    
        "term",               
        "total_credit"        
    )

# Hiển thị kết quả
final_df.show()

# Lưu kết quả vào danh sách để xử lý
scholarship = final_df.collect()
schema = final_df.schema
# In thông tin sinh viên
table = tb.create(result=scholarship,schema=schema)
for row in table:
    print(row)
# Đóng Spark session
spark.stop()
