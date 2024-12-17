import tkinter as tk
from tkinter import ttk
import failedStudents
import subjectAverage

# Tạo giao diện tkinter
root = tk.Tk()
root.title("Ứng dụng Spark vào Cassandra")
root.geometry("800x600")

# Tạo một Notebook để quản lý các tab
notebook = ttk.Notebook(root)
notebook.pack(fill=tk.BOTH, expand=True)

# Tab 1: Thêm dữ liệu
tab1 = ttk.Frame(notebook)
notebook.add(tab1, text="Thêm dữ liệu")

# Tab 2: Truy vấn
tab2 = ttk.Frame(notebook)
notebook.add(tab2, text="Truy vấn")

# ---- Nội dung cho Tab 1: Thêm dữ liệu ----
def add_data():
    print("Thêm dữ liệu vào Cassandra")

add_button = tk.Button(tab1, text="Thêm dữ liệu", command=add_data)
add_button.pack(pady=10)

# ---- Nội dung cho Tab 2: Truy vấn ----

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

# Biến để lưu trữ truy vấn đã chọn
selected_query_key = tk.StringVar()

# Tạo Frame để chứa Combobox và Button trên cùng một dòng
query_frame = tk.Frame(tab2)
query_frame.pack(fill=tk.X, padx=10, pady=10)

# Tạo Combobox để hiển thị mô tả truy vấn, giá trị là các khóa trong từ điển queries
query_combobox = ttk.Combobox(query_frame, textvariable=selected_query_key, values=list(queries.values()))
# query_combobox.set(list(queries.values())[0])  # Đặt giá trị mặc định là mô tả của truy vấn đầu tiên
query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

# Nút chạy truy vấn nằm trên cùng một dòng với Combobox
run_button = tk.Button(query_frame, text="Chạy truy vấn", command=lambda: select())
run_button.pack(side=tk.LEFT, padx=5)

# Tạo từ điển ánh xạ tên truy vấn với các hàm tương ứng (bạn có thể thêm các truy vấn khác vào đây)
query_functions = {
    "querie1": failedStudents.run_spark_job,
    "querie2": subjectAverage.run_spark_job
    # Bạn có thể thêm các hàm khác tương ứng với từng truy vấn
}

def table(truyVan):
    title_column, mang = truyVan()

    tree = ttk.Treeview(tab2, columns=title_column, show='headings')

    for col in title_column:
        tree.heading(col, text=col)
        tree.column(col, width=80, anchor=tk.CENTER)

    # Thêm các dòng kết quả vào bảng
    for row in mang:
        columns = [pair.split(": ")[1] for pair in row.split(", ")]
        tree.insert('', tk.END, values=columns)
    return tree

def select():
    # Xóa bảng cũ nếu có
    for widget in tab2.winfo_children():
        if isinstance(widget, ttk.Treeview):
            widget.destroy()

    # Lấy mô tả đã chọn từ Combobox và tìm khóa tương ứng trong từ điển queries
    selected_description = selected_query_key.get()

    # Tìm khóa của mô tả trong từ điển queries
    query_key = next((key for key, value in queries.items() if value == selected_description), None)

    # Gọi hàm tương ứng từ từ điển query_functions
    if query_key in query_functions:
        query_function = query_functions[query_key]
        tree = table(query_function)
        tree.pack(fill=tk.BOTH, expand=True)

# Khởi động giao diện
root.mainloop()