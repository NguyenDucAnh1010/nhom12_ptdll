import tkinter as tk
import subprocess

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job(selected_table=None, column_names=None,search_label=None):
    # Chạy lệnh spark-submit với truy vấn SQL được chọn
    command = f"docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/SearchQuery.py --selected_table {selected_table} --column_names {column_names} --search_label {search_label}\""
    # spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/SearchQuery.py --selected_table department --search_label KT
    
    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        # Lọc bỏ các dòng không phải kết quả 
        # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa ID sinh viên
        output_lines = result.stdout.split("\n")
        filtered_output = [line for line in output_lines if line.startswith(column_names)]  # Đây là mảng
        
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        return filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]