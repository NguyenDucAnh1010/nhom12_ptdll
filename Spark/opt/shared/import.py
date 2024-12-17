from pyspark.sql import SparkSession
from pyspark import SparkConf

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("Import") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Danh sách file và bảng tương ứng
files_and_tables = {
    "/opt/shared/datapreprocessing/department.csv": "department",
    "/opt/shared/datapreprocessing/class.csv": "class",
    "/opt/shared/datapreprocessing/subject.csv": "subject",
    "/opt/shared/datapreprocessing/student.csv": "student",
    "/opt/shared/datapreprocessing/grade.csv": "grade"
}

# Schema cho từng bảng
schemas = {
    "department": "iddepartment STRING, namedepartment STRING",
    "class": "idclass STRING, nameclass STRING, iddepartment STRING",
    "subject": "idsubject STRING, namesubject STRING, credit INT",
    "student": "idstudent STRING, namestudent STRING, phonenumber STRING, address STRING, idclass STRING",
    "grade": "idstudent STRING, idsubject STRING, term INT, grade FLOAT"
}

# Vòng lặp xử lý từng file và ghi vào từng bảng
for file_path, table_name in files_and_tables.items():
    schema = schemas[table_name]
    
    # Đọc file CSV vào DataFrame và ghi vào Cassandra
    spark.read.csv(file_path, schema=schema, header=True) \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table_name, keyspace="nhom12") \
        .save()

# Đóng Spark session
spark.stop()