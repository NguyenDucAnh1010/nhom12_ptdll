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
        "querie5": "top N sinh viên với …"
    }
    selected_query = tk.StringVar(root)

    # Tạo PanedWindow chia 2 bên
    paned_window = tk.PanedWindow(root, orient=tk.HORIZONTAL)
    paned_window.pack(fill=tk.BOTH, expand=True)

    # Tạo frame bên trái cho phần nhập dữ liệu
    left_frame = tk.Frame(paned_window, bg='lightgray')
    paned_window.add(left_frame)

    # Tạo Frame cho hàng đầu tiên gồm nút Thoát và Label
    top_left_frame = tk.Frame(left_frame, bg='lightgray')
    top_left_frame.pack(fill=tk.X, padx=10, pady=5)

    # Nút Thoát nằm bên trái Label
    exit_button = tk.Button(top_left_frame, text="Thoát", command=lambda: exit_query())
    exit_button.pack(side=tk.LEFT, padx=5)

    # Label nằm sau nút Thoát
    tk.Label(top_left_frame, text="Nhập dữ liệu vào bảng:", bg='lightgray').pack(side=tk.LEFT, padx=10)

    # Tạo Frame cho hàng thứ hai với Label và Entry cùng dòng
    input_frame = tk.Frame(left_frame, bg='lightgray')
    input_frame.pack(fill=tk.X, padx=10, pady=5)

    input_label = tk.Label(input_frame, text="Tên sinh viên:", bg='lightgray')
    input_label.pack(side=tk.LEFT)

    input_entry = tk.Entry(input_frame)
    input_entry.pack(side=tk.LEFT, padx=10, fill=tk.X, expand=True)

    # Nút thêm dữ liệu nằm dưới
    insert_button = tk.Button(left_frame, text="Thêm vào bảng", command=lambda: insert_data())
    insert_button.pack(padx=10, pady=10)

    # Tạo frame bên phải cho combobox và bảng kết quả
    right_frame = tk.Frame(paned_window)
    paned_window.add(right_frame)

    # Tạo Frame chứa Combobox và Button
    query_frame = tk.Frame(right_frame)
    query_frame.pack(fill=tk.X, padx=10, pady=10)

    # Combobox hiển thị mô tả truy vấn
    query_combobox = ttk.Combobox(query_frame, textvariable=selected_query, values=list(queries.values()))
    query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

    # Nút chạy truy vấn
    run_button = tk.Button(query_frame, text="Chạy truy vấn", command=lambda: select_query())
    run_button.pack(side=tk.LEFT, padx=5)

    # Tạo từ điển ánh xạ tên truy vấn với các hàm tương ứng
    query_functions = {
        "querie9": run_spark_job
    }

    def create_table(query_function):
        title_column, data = query_function()

        tree = ttk.Treeview(right_frame, columns=title_column, show='headings')

        for col in title_column:
            tree.heading(col, text=col)
            tree.column(col, width=80)

        for row in data:
            columns = [pair.split(": ")[1] for pair in row.split(", ")]
            tree.insert('', tk.END, values=columns)

        return tree

    def select_query():
        # Xóa bảng cũ nếu có
        for widget in right_frame.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()

        # Lấy mô tả đã chọn từ Combobox và tìm khóa tương ứng trong từ điển queries
        selected_description = selected_query.get()
        query_key = next((key for key, value in queries.items() if value == selected_description), None)

        # Gọi hàm tương ứng từ từ điển query_functions
        if query_key in query_functions:
            query_function = query_functions[query_key]
            tree = create_table(query_function)
            tree.pack(fill=tk.BOTH, expand=True)

    def insert_data():
        # Lấy dữ liệu từ Entry và thêm vào bảng
        new_data = input_entry.get()
        # Thực hiện thao tác thêm dữ liệu vào bảng (cần tùy chỉnh theo logic)
        print(f"Dữ liệu thêm: {new_data}")  # In ra để kiểm tra

    def exit_query():
        root.destroy()  # Đóng giao diện hiện tại
        if home_callback:
            home_callback()  # Gọi lại hàm từ home

    # Khởi động giao diện
    root.mainloop()
