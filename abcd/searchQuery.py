import tkinter as tk
import subprocess
from func.table import Table as tb
from func.roundNumber import roundData
# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job(**kwargs):
    # Chạy lệnh spark-submit với truy vấn SQL được chọn
    selected_table=kwargs.get("selected_table")
    column_names=kwargs.get("column_names")
    search_label=kwargs.get("search_label")
    command = f"docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/SearchQuery.py --selected_table {selected_table} --search_label {search_label}\""
    # spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/SearchQuery.py --selected_table department --search_label KT
    
    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        # Lọc bỏ các dòng không phải kết quả 
        # # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa ID sinh viên
        # Lọc bỏ các dòng không phải kết quả 
        # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa ID sinh viên
        output_lines = result.stdout.split("\n")
        arr = tb.convert_data(output_lines)
        arr = roundData(arr)
        return arr
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]