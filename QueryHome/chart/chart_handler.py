import abcd.failedStudentsChart as failedStudentsChart

class ChartHandler:
    def __init__(self):
        # Từ điển ánh xạ giữa truy vấn và hàm vẽ biểu đồ
        self.query_chart_functions = {
            "querie9": failedStudentsChart.draw_chart  # Ví dụ ánh xạ truy vấn tới hàm vẽ biểu đồ
        }

    def display_chart(self, query_key, *args):
        if query_key in self.query_chart_functions:
            # Gọi hàm tương ứng từ từ điển query_chart_functions để vẽ biểu đồ
            chart_function = self.query_chart_functions[query_key]
            chart_function(*args)  # Hàm này sẽ thực hiện việc vẽ biểu đồ
        else:
            print("Không tìm thấy truy vấn tương ứng để vẽ biểu đồ.")
