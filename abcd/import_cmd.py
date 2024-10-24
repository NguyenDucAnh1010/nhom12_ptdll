import tkinter as tk
import subprocess

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job():
    # Chạy lệnh spark-submit với truy vấn SQL được chọn
    command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/import.py\""
    
    try:
        subprocess.run(command, shell=True, capture_output=True, text=True)

        # Xóa thông báo "Loading" sau khi hoàn thành
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]