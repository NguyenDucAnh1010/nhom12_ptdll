import subprocess
import pandas as pd
def run_spark_job(selected_class=None):
    try:
        file_path = "/opt/shared/dataset/gpa_for_class"
        command = f"docker exec -it spark-master bash -c \"spark-submit --master spark://spark-master:7077 \
                            --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
                            /opt/shared/GPA_avg_of_student_in_class.py --selected_class ${selected_class}"

        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output_lines = result.stdout.split("\n")
        filtered_output = [line for line in output_lines if line.startswith("Row")]
        if not filtered_output:
            raise ValueError("Không tìm thấy kết quả phù hợp")
        # else:
        #     df = pd.read_csv(file_path+"part-00000.csv")
        #     print(df)
        print(filtered_output)
        return filtered_output
    except Exception as e:
        return f"An error occurred: {str(e)}"