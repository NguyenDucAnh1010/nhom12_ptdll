import subprocess

def run_spark_job():
    # Chạy lệnh spark-submit với truy vấn SQL được chọn
    command = "docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 /opt/shared/FailedStudents.py\""

    try:
        # Chạy lệnh docker exec và thu thập stdout
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        # Kiểm tra nếu result.stdout là None
        if result.stdout is None:
            raise ValueError("Không có đầu ra từ lệnh.")

        # Lọc bỏ các dòng không phải kết quả 
        output_lines = result.stdout.split("\n")
        print(result.stdout)

        # Tìm kiếm dòng chứa ID sinh viên
        filtered_output = [line for line in output_lines if line.startswith("ID sinh viên")]
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")

        title_column = ["ID sinh viên", "Tên sinh viên", "Mã môn học", "Tên môn học", "Điểm"]

        return title_column, filtered_output
    except Exception as e:
        return [], [f"Lỗi khi thực thi: {e}"]
