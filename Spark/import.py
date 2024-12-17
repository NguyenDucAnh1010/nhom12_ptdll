from pyspark.sql import SparkSession
from pyspark import SparkConf
from cassandra.cluster import Cluster
from dataclasses import dataclass

# Định nghĩa các lớp Python bằng dataclass
@dataclass
class Department:
    iddepartment: str
    namedepartment: str

@dataclass
class Classes:
    idclass: str
    nameclass: str
    iddepartment: str
    namedepartment: str

@dataclass
class Student:
    idstudent: str
    namestudent: str
    phonenumber: str
    address: str
    idclass: str
    nameclass: str
    iddepartment: str
    namedepartment: str

@dataclass
class Subject:
    idsubject: str
    namesubject: str
    credit: int

@dataclass
class Grade:
    idstudent: str
    namestudent: str
    idsubject: str
    namesubject: str
    grade: float

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("MapReduce") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Khởi tạo kết nối đến Cassandra
cluster = Cluster(['127.0.0.1'])  # Sử dụng tên container hoặc địa chỉ IP của Cassandra
session = cluster.connect('nhom12')  # Kết nối tới keyspace nhom12

# Danh sách file và bảng tương ứng
files = ["\opt\shared\datapreprocessing\Khoa_cleaned.csv", 
        "\opt\Spark\shared\datapreprocessing\Lop_cleaned.csv", 
        "\opt\Spark\shared\datapreprocessing\MonHoc_cleanedHoc.csv", 
        "\opt\Spark\shared\datapreprocessing\SV_cleaned.csv",
        "\opt\Spark\shared\datapreprocessing\Diem_cleaned.csv"]
tables = ["department", "class", "subject", "student", "grade"]

# Vòng lặp xử lý từng file và ghi vào từng bảng
for i, file_path in enumerate(files):
    table_name = tables[i]
    
    # Đọc file CSV từ local hoặc HDFS (trong trường hợp dùng HDFS)
    lines = spark.read.text(file_path).rdd.map(lambda row: row[0])
    
    # Bỏ qua hàng đầu tiên (tên cột)
    data_without_header = lines.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
    
    # Tách dữ liệu CSV và chuyển sang đối tượng Python
    if i == 0:  # department
        departments = data_without_header.map(lambda line: line.split(",")) \
            .map(lambda parts: Department(parts[0], parts[1]))
        # Ghi vào Cassandra
        for row in departments.collect():
            session.execute("INSERT INTO nhom12.department (iddepartment, namedepartment) VALUES (%s, %s)", (row.iddepartment, row.namedepartment))
    
    elif i == 1:  # class
        classes = data_without_header.map(lambda line: line.split(",")) \
            .map(lambda parts: Classes(parts[0], parts[1], parts[2], parts[3]))
        # Ghi vào Cassandra
        for row in classes.collect():
            session.execute("INSERT INTO nhom12.class (idclass, nameclass, iddepartment, namedepartment) VALUES (%s, %s, %s, %s)", 
                            (row.idclass, row.nameclass, row.iddepartment, row.namedepartment))
    
    elif i == 2:  # subject
        subjects = data_without_header.map(lambda line: line.split(",")) \
            .map(lambda parts: Subject(parts[0], parts[1], int(parts[2])))
        # Ghi vào Cassandra
        for row in subjects.collect():
            session.execute("INSERT INTO nhom12.subject (idsubject, namesubject, credit) VALUES (%s, %s, %s)", 
                            (row.idsubject, row.namesubject, row.credit))
    
    elif i == 3:  # student
        students = data_without_header.map(lambda line: line.split(",")) \
            .map(lambda parts: Student(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]))
        # Ghi vào Cassandra
        for row in students.collect():
            session.execute("INSERT INTO nhom12.student (idstudent, namestudent, phonenumber, address, idclass, nameclass, iddepartment, namedepartment) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                            (row.idstudent, row.namestudent, row.phonenumber, row.address, row.idclass, row.nameclass, row.iddepartment, row.namedepartment))
    
    elif i == 4:  # grade
        grades = data_without_header.map(lambda line: line.split(",")) \
            .map(lambda parts: Grade(parts[0], parts[1], parts[2], parts[3], float(parts[4])))
        # Ghi vào Cassandra
        for row in grades.collect():
            session.execute("INSERT INTO nhom12.grade (idstudent, namestudent, idsubject, namesubject, grade) VALUES (%s, %s, %s, %s, %s)", 
                            (row.idstudent, row.namestudent, row.idsubject, row.namesubject, row.grade))

# Đóng Spark session
spark.stop()