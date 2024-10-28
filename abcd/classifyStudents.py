import tkinter as tk
import subprocess
from func.table import Table as tb
from func.roundNumber import roundData
# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def classify_students(**kwargs):
    # Tạo lệnh spark-submit
    selected_subject=kwargs.get("selected_subject")
    command = f'docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/ClassifyStudent.py \'{selected_subject}\'"'
    print(command)

    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        # Lọc bỏ các dòng không phải kết quả
        output_lines = result.stdout.split("\n")
        arr = tb.convert_data(output_lines)
        arr = roundData(arr)
        return arr
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]
