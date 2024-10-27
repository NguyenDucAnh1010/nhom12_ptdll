import subprocess
import pandas as pd
from func.table import Table as tb
from func.roundNumber import roundData
def run_spark_job(**kwargs):
    try:
        selected_class = kwargs.get("selected_class",None)
        file_path = "/opt/shared/dataset/gpa_for_class"
        command = f"docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 \
                            --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
                            /opt/shared/GPA_avg_of_student_in_class.py --selected_class {selected_class}"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output_lines = result.stdout.split("\n")
        arr = tb.convert_data(output_lines)
        arr = roundData(arr)
        return arr
    except Exception as e:
        return f"Có lỗi xảy ra khi thực thi : {str(e)}"