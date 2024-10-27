import tkinter as tk
import subprocess
from func.table import Table as tb
from func.roundNumber import roundData

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def failed_students(**kawargs):
    # Kiểm tra số lượng và giá trị của các tham số truyền vào
    if len(kawargs) >= 2:
        name_department = kawargs.get("department")
        name_class = kawargs.get("class_name")
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudents.py '{0}' '{1}'\"".format(name_department, name_class)
    else:
        name_department = kawargs.get("department")
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudents.py '{0}'\"".format(name_department)
    
    print(command)
    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        # Lọc bỏ các dòng không phải kết quả
        # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa ID sinh viên
        output_lines = result.stdout.split("\n")
        arr = tb.convert_data(output_lines)
        arr = roundData(arr)
        return arr
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]