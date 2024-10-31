import query_Spark.failedStudents as failedStudents
import query_Spark.gpa_avg_of_student as gpa_avg_of_student
import query_Spark.scholarship as scholarship
import query_Spark.classifyStudents as classifyStudents
import query_Spark.top10 as top10
import query_Spark.sumStudentPass as sumStudentPass
import query_Spark.gpa_credit_avgChart as gpa_credit_avgChart
import query_Spark.academicWarning as acacademicWarning
class QueryExecutor:
    def __init__(self, queries):
        self.queries = queries
        self.query_functions = {
            "querie5": gpa_credit_avgChart.run_spark_job,
            "querie6": gpa_avg_of_student.run_spark_job,
            "querie7": failedStudents.failed_students,# Ánh xạ truy vấn tới hàm xử lý tương ứng
            "querie1": scholarship.scholarship_students  ,
            "querie2": classifyStudents.classify_students  ,
            "querie3": top10.run_spark_job,
            "querie4": sumStudentPass.run_spark_job,
            "querie8": acacademicWarning.academic_warning
        }

    def execute_query(self, query_key,*args,**kwargs):
        if query_key in self.query_functions:
            # Gọi hàm tương ứng từ từ điển query_functions với các tham số truyền vào
            query_function = self.query_functions[query_key]
            # Truyền tham số khoa và lớp vào
            return query_function(**kwargs)  # Trong trường hợp chi can tham so la khoa
        return None


# Co the dung cac truong hop sau de bat xem tham so co None khong
# Truyền các tham số, chỉ truyền những tham số có giá trị
        # if department and class_name and semester and student_id:
        #     return query_function(department, class_name, semester, student_id)
        # elif department and class_name and semester:
        #     return query_function(department, class_name, semester)
        # elif department and class_name:
        #     return query_function(department, class_name)
        # else:
        #     return query_function(department)  # Chỉ truyền department nếu không có các tham số khác