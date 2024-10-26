import tkinter as tk
import subprocess
import matplotlib.pyplot as plt
import pandas as pd

# Hàm thực thi lệnh spark-submit bên trong container Docker và chỉ lấy kết quả
def run_spark_job(*args):
    # Chạy lệnh spark-submit với truy vấn SQL được chọn    
    if len(args) >= 2:
        name_department = args[0]
        name_class = args[1]
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudentsChart.py '{0}' '{1}'\"".format(name_department, name_class)
    else:
        name_department = args[0]
        command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudentsChart.py '{0}' \"".format(name_department)
    
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
    
import pandas as pd

def process_data_to_dataframe(data):
    # Tách dữ liệu thành các phần tử
    processed_data = []
    for entry in data:
        parts = entry.split(', ')
        department = parts[0].split(': ')[1]
        class_name = parts[1].split(': ')[1]
        term = int(parts[2].split(': ')[1])
        failed_rate = float(parts[3].split(': ')[1])
        processed_data.append([department, class_name, term, failed_rate])
    
    # Tạo DataFrame
    df = pd.DataFrame(processed_data, columns=["Department", "Class", "Term", "FailedRate"])
    
    # Pivot theo 'Class' và 'Term'
    df_pivot = df.pivot(index='Class', columns='Term', values='FailedRate')
    
    return df_pivot

def draw_chart(*args):
    data_lines = run_spark_job(*args)
    processed_dataframe = process_data_to_dataframe(data_lines)

    # Vẽ biểu đồ cột
    ax = processed_dataframe.plot(kind='bar', figsize=(10, 6), width=0.5)

    # Đặt tiêu đề và nhãn trục
    ax.set_title('Tỷ lệ trượt môn theo lớp và kỳ', fontsize=14)  # Cập nhật tiêu đề cho đúng nội dung
    ax.set_xlabel('Tên Lớp', fontsize=12)  # Cập nhật lại trục x cho phù hợp
    ax.set_ylabel('Tỷ lệ trượt môn (%)', fontsize=12)

    # Định dạng trục y thành phần trăm
    # ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.0%}'))

    # Hiển thị biểu đồ
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
