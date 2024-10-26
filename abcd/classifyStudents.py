import tkinter as tk
import subprocess

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def classify_students(selected_subject=None):
    # Tạo lệnh spark-submit
    command = f'docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/ClassifyStudent.py \'{selected_subject}\'"'
    print(command)

    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        # Lọc bỏ các dòng không phải kết quả
        output_lines = result.stdout.split("\n")
        filtered_output = [line for line in output_lines if line.startswith("ID sinh viên")]  
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        title_column = ["Mã sinh viên", "Tên sinh viên", "Mã môn học", "Tên môn học", "Điểm", "Điểm trung bình", "Phân loại"]

        return title_column, filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]
