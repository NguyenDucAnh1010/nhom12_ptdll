import tkinter as tk
from tkinter import ttk
from abcd.failedStudents import run_spark_job

def query(home_callback=None):
    # Tạo giao diện tkinter
    root = tk.Tk()
    root.title("Ứng dụng Spark vào Cassandra")
    root.geometry("800x600")

    # Từ điển queries với khóa và mô tả truy vấn
    queries = {
        "querie1": "tính điểm trung bình môn theo từng môn",
        "querie2": "sinh viên có điểm cao || thấp cao nhất",
        "querie3": "danh sách sinh viên || môn …",
        "querie4": "môn || lớp … có nhiều sinh viên nhất",
        "querie5": "top N sinh viên với …",
        "querie6": "xóa 1 cột thông tin trong sinhvien",
        "querie7": "thêm cột thông tin trong sv/lop/khoa/diem",
        "querie8": "tính điểm gpa của từng sinh viên",
        "querie9": "thống kê số sinh viên trượt môn (< 4)",
        "querie10": "tổng sinh viên theo khoa || lớp",
    }
    selected_query = tk.StringVar(root)
    # query_combobox.set(list(queries.values())[0])  # Đặt giá trị mặc định là mô tả của truy vấn đầu tiên

    # Tạo Frame để chứa Combobox và Button trên cùng một dòng
    query_frame = tk.Frame(root)
    query_frame.pack(fill=tk.X, padx=10, pady=10)

    # Tạo nút Thoát và đặt nó nằm trước Combobox, cùng dòng
    exit_button = tk.Button(query_frame, text="Thoát", command=lambda: exit_query())
    exit_button.pack(side=tk.LEFT, padx=5)

    # Tạo Combobox để hiển thị mô tả truy vấn, giá trị là các khóa trong từ điển queries
    query_combobox = ttk.Combobox(query_frame, textvariable=selected_query, values=list(queries.values()))
    query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

    # Nút chạy truy vấn nằm trên cùng một dòng với Combobox, ngay sau Combobox
    run_button = tk.Button(query_frame, text="Chạy truy vấn", command=lambda: select_query())
    run_button.pack(side=tk.LEFT, padx=5)

    # Tạo từ điển ánh xạ tên truy vấn với các hàm tương ứng
    query_functions = {
        "querie9": run_spark_job
    }

    def create_table(query_function):
        title_column, data = query_function()

        tree = ttk.Treeview(root, columns=title_column, show='headings')

        for col in title_column:
            tree.heading(col, text=col)
            tree.column(col, width=80)
        
        for row in data:
            columns = [pair.split(": ")[1] for pair in row.split(", ")]
            tree.insert('', tk.END, values=columns)

        return tree

    def select_query():
        # Xóa bảng cũ nếu có
        for widget in root.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()

        # Lấy mô tả đã chọn từ Combobox và tìm khóa tương ứng trong từ điển queries
        selected_description = selected_query.get()

        # Tìm khóa của mô tả trong từ điển queries
        query_key = next((key for key, value in queries.items() if value == selected_description), None)

        # Gọi hàm tương ứng từ từ điển query_functions
        if query_key in query_functions:
            query_function = query_functions[query_key]
            tree = create_table(query_function)
            tree.pack(fill=tk.BOTH, expand=True)

    def exit_query():
        root.destroy()  # Đóng giao diện hiện tại
        if home_callback:
            home_callback()  # Gọi lại hàm từ home

    # Khởi động giao diện
    root.mainloop()
