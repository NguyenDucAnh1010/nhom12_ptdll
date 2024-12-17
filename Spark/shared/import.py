from pyspark.sql import SparkSession
from pyspark import SparkConf

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("MapReduce") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Danh sách file và bảng tương ứng
files = [
    "/opt/shared/datapreprocessing/Khoa_cleaned.csv", 
    "/opt/shared/datapreprocessing/Lop_cleaned.csv", 
    "/opt/shared/datapreprocessing/MonHoc_cleaned.csv", 
    "/opt/shared/datapreprocessing/SV_cleaned.csv",
    "/opt/shared/datapreprocessing/Diem_cleaned.csv"
]
tables = ["department", "class", "subject", "student", "grade"]

# Schema cho từng bảng
schemas = {
    "department": "iddepartment STRING, namedepartment STRING",
    "class": "idclass STRING, nameclass STRING, iddepartment STRING, namedepartment STRING",
    "subject": "idsubject STRING, namesubject STRING, credit INT",
    "student": "idstudent STRING, namestudent STRING, phonenumber STRING, address STRING, idclass STRING, nameclass STRING, iddepartment STRING, namedepartment STRING",
    "grade": "idstudent STRING, namestudent STRING, idsubject STRING, namesubject STRING, grade FLOAT"
}

# Vòng lặp xử lý từng file và ghi vào từng bảng
for i, file_path in enumerate(files):
    table_name = tables[i]
    schema = schemas[table_name]

    # Đọc file CSV vào DataFrame
    df = spark.read.csv(file_path, schema=schema, header=True)

    # Ghi dữ liệu vào Cassandra
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table_name, keyspace="nhom12") \
        .save()

# Đóng Spark session
spark.stop()