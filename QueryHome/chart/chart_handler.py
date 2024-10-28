import abcd.failedStudentsChart as failedStudentsChart
import abcd.scholarship as scholarship
import abcd.classifyStudents as classifyStudents
import abcd.top10 as top10
import abcd.sumStudentPass as sumStudentPasss
import abcd.gpa_credit_avgChart as gpa_credit_avgChart
import abcd.gpa_avg_of_student as gpa_avg_of_student
class ChartHandler:
    def __init__(self):
        # Từ điển ánh xạ giữa truy vấn và hàm vẽ biểu đồ
        self.query_chart_functions = {
            "querie1": scholarship.scholarship_students,
            "querie2": classifyStudents.classify_students,
            "querie3": top10.run_spark_job,
            "querie4": sumStudentPasss.run_spark_job,
            "querie5": gpa_credit_avgChart.run_spark_job,
            "querie6": gpa_avg_of_student.run_spark_job,
            "querie7": failedStudentsChart.run_spark_job , # Ví dụ ánh xạ truy vấn tới hàm vẽ biểu đồ
        }

    def get_data(self, query_key, **kwargs):
        if query_key in self.query_chart_functions:
            # Gọi hàm tương ứng từ từ điển query_chart_functions để vẽ biểu đồ
            chart_function = self.query_chart_functions[query_key]
            result = chart_function(**kwargs)  # Hàm này sẽ thực hiện việc vẽ biểu đồ]
            return result
        else:
            print("Không tìm thấy truy vấn tương ứng để vẽ biểu đồ.")
            return None
