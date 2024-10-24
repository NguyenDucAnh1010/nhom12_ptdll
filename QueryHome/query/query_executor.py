import abcd.failedStudents as failedStudents

class QueryExecutor:
    def __init__(self, queries):
        self.queries = queries
        self.query_functions = {
            "querie9": failedStudents.run_spark_job  # Ánh xạ truy vấn tới hàm xử lý tương ứng
        }

    def execute_query(self, query_key):
        if query_key in self.query_functions:
            # Gọi hàm tương ứng từ từ điển query_functions và trả về kết quả
            query_function = self.query_functions[query_key]
            return query_function()  # Hàm này sẽ trả về dữ liệu dạng (title_column, data)
        return None
