import tkinter as tk
import subprocess
import matplotlib.pyplot as plt
import pandas as pd

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job():
    # Chạy lệnh spark-submit với truy vấn SQL được chọn
    command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudentsChart.py\""
    
    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        # Lọc bỏ các dòng không phải kết quả 
        # Tách các dòng dựa trên dấu phẩy hoặc khoảng trắng, sau đó lọc ra các dòng chứa Khoa
        output_lines = result.stdout.split("\n")
        # filtered_output = "\n".join([line for line in output_lines if line.startswith("Khoa")])
        filtered_output = [line for line in output_lines if line.startswith("Khoa")]  # Đây là mảng
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        return filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]
    
def process_data_to_dataframe(data):
    # Tách dữ liệu thành các phần tử
    processed_data = []
    for entry in data:
        parts = entry.split(', ')
        department = parts[0].split(': ')[1]
        term = parts[1].split(': ')[1]
        failed_rate = float(parts[2].split(': ')[1])
        processed_data.append([department, term, failed_rate])
    
    # Tạo DataFrame
    df = pd.DataFrame(processed_data, columns=["Department", "Term", "FailedRate"])
    df_pivot = df.pivot(index='Department', columns='Term', values='FailedRate')
    
    return df_pivot

def draw_chart():
    data_lines = run_spark_job()
    # print(bieudo1)
    processed_dataframe = process_data_to_dataframe(data_lines)
    # print(df_pivot)

    # Vẽ biểu đồ cột
    ax = processed_dataframe.plot(kind='bar', figsize=(10, 6), width=0.8)

    # Đặt tiêu đề và nhãn trục
    ax.set_title('Tỷ lệ trượt môn theo khoa và kỳ', fontsize=14)
    ax.set_xlabel('Tên Khoa', fontsize=12)
    ax.set_ylabel('Tỷ lệ trượt môn (%)', fontsize=12)

    # Hiển thị biểu đồ
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
