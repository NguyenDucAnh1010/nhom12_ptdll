import tkinter as tk
import subprocess

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job(subject_name):
    # Chạy lệnh spark-submit với mã môn học
    command = f"docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/top10query.py '{subject_name}'\""
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        output_lines = result.stdout.split("\n")
        filtered_output = [line for line in output_lines if line.startswith("ID sinh viên")]
        print(filtered_output)
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        title_column = ["ID sinh viên", "Tên sinh viên", "Mã môn học", "Tên môn học", "Điểm"]

        return title_column, filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]



