import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
from pyspark.sql import Window
from func.table import Table as tb

# Nhận tham số dòng lệnh
name_department = sys.argv[1] if len(sys.argv) > 1 else None

# Đặt ngưỡng điểm
grade_threshold = 2.5

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("AcademicWarning") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

# Tạo Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Đọc dữ liệu từ Cassandra
try:
    grades_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="grade", keyspace="nhom12") \
        .load()
    
    subjects_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="subject", keyspace="nhom12") \
        .load()

    students_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="student", keyspace="nhom12") \
        .load()

    class_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="class", keyspace="nhom12") \
        .load()

    department_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="department", keyspace="nhom12") \
        .load()

except Exception as e:
    print(f"Lỗi khi tải dữ liệu từ Cassandra: {str(e)}")
    spark.stop()
    sys.exit(1)

# Join các bảng để lấy thông tin khoa và lớp
students_with_class_dept_df = students_df \
    .join(class_df, "idclass", "left") \
    .join(department_df, "iddepartment", "left")

# Lọc theo khoa và lớp dựa trên thông tin nhập vào
filtered_students_df = students_with_class_dept_df \
    .filter(F.col("namedepartment") == name_department)

# Lấy danh sách các idstudent phù hợp
filtered_student_ids_df = filtered_students_df.select("idstudent")

# Join với bảng điểm chỉ với sinh viên đã lọc
filtered_grades_df = grades_df.join(filtered_student_ids_df, "idstudent", "inner")

# Join bảng Grade với Subject để lấy số tín chỉ
filtered_grades_with_credits_df = filtered_grades_df \
    .join(subjects_df, "idsubject", "left")

# Chuyển đổi điểm từ hệ 10 sang hệ số 4
filtered_grades_with_credits_df = filtered_grades_with_credits_df \
    .withColumn("grade_on_4_scale", 
                F.when(F.col("grade") >= 8.5, 4.0)
                .when(F.col("grade") >= 7.0, 3.0)
                .when(F.col("grade") >= 5.5, 2.0)
                .when(F.col("grade") >= 4.0, 1.0)
                .otherwise(0.0))

# Tính GPA hệ số 4 cho từng kỳ của mỗi sinh viên
gpa_per_term_df = filtered_grades_with_credits_df \
    .groupBy("idstudent", "term") \
    .agg(
        (F.sum(F.col("grade_on_4_scale") * F.col("credit")) / F.sum("credit")).alias("gpa_per_term")
    )

# Xác định 3 kỳ gần nhất của mỗi sinh viên để kiểm tra xu hướng giảm
window_spec = Window.partitionBy("idstudent").orderBy("term")
recent_terms_df = gpa_per_term_df \
    .withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") <= 3) \
    .orderBy("idstudent", "term")

# Đếm tổng số kỳ cho mỗi sinh viên
total_terms_df = recent_terms_df.groupBy("idstudent").agg(F.count("term").alias("total_terms"))
# Chỉ giữ lại sinh viên có ít nhất 2 kỳ để kiểm tra xu hướng
recent_terms_df = recent_terms_df.join(total_terms_df, "idstudent").filter(F.col("total_terms") >= 2)

# Sắp xếp lại theo thứ tự kỳ tăng dần để tính xu hướng cho 3 kỳ gần nhất
# recent_terms_df = recent_terms_df.orderBy("idstudent", "term")
recent_terms_df = recent_terms_df.orderBy("idstudent", "term")

# Tính sự khác biệt điểm giữa các kỳ và xác định kỳ gần nhất
recent_terms_df = recent_terms_df \
    .withColumn("grade_difference", F.col("gpa_per_term") - F.lag("gpa_per_term").over(window_spec)) \
    .withColumn("is_decreasing", F.when(F.col("grade_difference") < 0, 1).otherwise(0)) \
    .withColumn("last_term_gpa", F.last("gpa_per_term").over(Window.partitionBy("idstudent")))

# Tính số kỳ giảm điểm và lọc theo điều kiện kỳ gần nhất
at_risk_students_df = recent_terms_df.groupBy("idstudent") \
    .agg(
        F.sum("is_decreasing").alias("decreasing_terms"),
        F.max("last_term_gpa").alias("last_term_gpa")
    ) \
    .filter((F.col("decreasing_terms") >= 2) & (F.col("last_term_gpa") < grade_threshold))

# Kết hợp kết quả nguy cơ với GPA của từng kỳ của sinh viên
result_df = at_risk_students_df \
    .join(students_with_class_dept_df, "idstudent", "left") \
    .join(gpa_per_term_df, "idstudent", "left") \
    .select("idstudent", "namestudent", "nameclass", "namedepartment", "term", "gpa_per_term", "decreasing_terms") \
    .orderBy("idstudent", "term")

# Chuyển đổi DataFrame `result_df` thành một tập hợp để có thể duyệt qua từng dòng
result = result_df.collect()
schema = result_df.schema

table = tb.create(result=result,schema=schema)

for row in table:
    print(row)
    
# Đóng Spark session
spark.stop()
