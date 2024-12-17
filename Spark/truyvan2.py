from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("TopBottomStudentsBySubject") \
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

# Đọc dữ liệu từ bảng student trong Cassandra
students_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="student", keyspace="nhom12") \
    .load()

# Tạo alias cho các DataFrame để tránh mơ hồ
grades_alias = grades_df.alias("grades")
students_alias = students_df.alias("students")

# Lấy sinh viên có điểm cao nhất cho mỗi môn học
highest_grades_df = grades_alias.groupBy("idsubject") \
    .agg(F.max("grade").alias("max_grade"))

# Lấy sinh viên có điểm thấp nhất cho mỗi môn học
lowest_grades_df = grades_alias.groupBy("idsubject") \
    .agg(F.min("grade").alias("min_grade"))

# Join để có thêm thông tin sinh viên cho điểm cao nhất
top_students_df = highest_grades_df.join(
    grades_alias,
    (highest_grades_df["idsubject"] == grades_alias["idsubject"]) &
    (highest_grades_df["max_grade"] == grades_alias["grade"])
).join(
    students_alias,
    grades_alias["idstudent"] == students_alias["idstudent"]
).select(
    grades_alias["idstudent"].alias("student_id"), 
    students_alias["namestudent"].alias("student_name"), 
    grades_alias["idsubject"].alias("subject_id"), 
    grades_alias["grade"].alias("grade")
)

# Join để có thêm thông tin sinh viên cho điểm thấp nhất
bottom_students_df = lowest_grades_df.join(
    grades_alias,
    (lowest_grades_df["idsubject"] == grades_alias["idsubject"]) &
    (lowest_grades_df["min_grade"] == grades_alias["grade"])
).join(
    students_alias,
    grades_alias["idstudent"] == students_alias["idstudent"]
).select(
    grades_alias["idstudent"].alias("student_id"), 
    students_alias["namestudent"].alias("student_name"), 
    grades_alias["idsubject"].alias("subject_id"), 
    grades_alias["grade"].alias("grade")
)

# Hiển thị kết quả sinh viên có điểm cao nhất
top_students_df.show()

# Hiển thị kết quả sinh viên có điểm thấp nhất
bottom_students_df.show()

# Đóng Spark session
spark.stop()