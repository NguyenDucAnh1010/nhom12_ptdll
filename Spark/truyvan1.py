from pyspark.sql import SparkSession
from pyspark import SparkConf

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("AverageGradesWithSubjectDetails") \
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

# Đọc dữ liệu từ bảng subject trong Cassandra
subjects_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="subject", keyspace="nhom12") \
    .load()

# Thực hiện join hai bảng dựa trên cột idsubject
joined_df = grades_df.join(subjects_df, grades_df['idsubject'] == subjects_df['idsubject']) \
    .select(grades_df['idsubject'], subjects_df['namesubject'], subjects_df['credit'], grades_df['grade'])

# Tính tổng điểm và số lượng sinh viên cho mỗi môn học
subject_totals_df = joined_df.groupBy("idsubject", "namesubject", "credit") \
    .agg({'grade': 'sum', '*': 'count'}) \
    .withColumnRenamed("sum(grade)", "total_grade") \
    .withColumnRenamed("count(1)", "student_count")

# Tính điểm trung bình
average_grades_df = subject_totals_df \
    .withColumn("average_grade", subject_totals_df["total_grade"] / subject_totals_df["student_count"])

# Thu thập kết quả để ghi vào file
average_grades = average_grades_df.collect()

for subject in average_grades:
    print(f"ID môn học: {subject['idsubject']}, Tên môn học: {subject['namesubject']}, Tín chỉ: {subject['credit']}, Điểm trung bình: {subject['average_grade']:.2f}\n")

# Đóng Spark session
spark.stop()
