import tkinter as tk
import subprocess

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def failed_students(*args):
    # Kiểm tra số lượng và giá trị của các tham số truyền vào
    if len(args) >= 2:
        name_department = args[0]
        name_class = args[1]
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudents.py '{0}' '{1}'\"".format(name_department, name_class)
    else:
        name_department = args[0]
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudents.py '{0}'\"".format(name_department)
    
    print(command)
    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        # Lọc bỏ các dòng không phải kết quả 
        # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa ID sinh viên
        output_lines = result.stdout.split("\n")
        # filtered_output = "\n".join([line for line in output_lines if line.startswith("ID sinh viên")])
        filtered_output = [line for line in output_lines if line.startswith("Khoa")]  # Đây là mảng
        # print("abc: ",filtered_output)
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        title_column = ["Khoa", "Lớp", "Môn", "Kỳ", "Mã sinh viên", "Tên sinh viên", "Điểm"]

        return title_column, filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]