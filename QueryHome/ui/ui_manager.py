import tkinter as tk
from tkinter import ttk, messagebox
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler

class UIManager:
    def __init__(self, root, queries, dictionary_department, cse_classes, kt_classes, ck_classes, home_callback=None):
        self.root = root
        self.root.title("Ứng dụng Spark vào Cassandra")
        self.root.geometry("800x600")
        self.queries = queries
        self.home_callback = home_callback

        # Tạo biến chuỗi cho các Combobox
        self.selected_query = tk.StringVar(root)
        self.selected_department = tk.StringVar(root)
        self.selected_class = tk.StringVar(root)
        self.dictionary_department = dictionary_department  

        # Các từ điển lớp
        self.cse_classes = cse_classes
        self.kt_classes = kt_classes
        self.ck_classes = ck_classes
        
        # Thiet lap cac thanh phan giao dien
        self.setup_ui(queries)

        # Xu ly su kien dong
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def setup_ui(self, queries):
        # Frames va button
        self.query_frame = tk.Frame(self.root)
        self.query_frame.pack(fill=tk.X, padx=10, pady=10)

        exit_button = tk.Button(self.query_frame, text="Thoát", command=self.exit_query)
        exit_button.pack(side=tk.LEFT, padx=5)

        # Query Combobox
        self.query_combobox = ttk.Combobox(self.query_frame, textvariable=self.selected_query, values=list(queries.values()))
        self.query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

        run_button = tk.Button(self.query_frame, text="Chạy truy vấn", command=self.execute_query)
        run_button.pack(side=tk.LEFT, padx=5)

        chart_button = tk.Button(self.query_frame, text="Biểu đồ", command=self.display_chart)
        chart_button.pack(side=tk.LEFT, padx=5)

        self.department_frame = None  # Placeholder for department combobox frame

        #Bat su kien khi hien thi cac combobox khoa va lop
        self.selected_query.trace("w", self.show_department_class_combobox)

    def show_department_class_combobox(self, *args):
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy() 

            # Nếu giá trị là "thống kê số sinh viên trượt môn (< 4)" thì hiển thị combobox
        if self.selected_query.get() == "thống kê số sinh viên trượt môn (< 4)":
            if self.department_frame is None:  # Kiểm tra nếu frame chưa được tạo
                self.department_frame = tk.Frame(self.root)
                self.department_frame.pack(fill=tk.X, padx=10, pady=0)

                # Tạo Combobox để hiển thị tên khoa
                self.department_combobox = ttk.Combobox(self.department_frame, textvariable=self.selected_department, values=list(self.dictionary_department.values()))
                self.department_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=2)

                # Tạo Combobox để hiển thị tên lớp
                self.class_combobox = ttk.Combobox(self.department_frame, textvariable=self.selected_class)
                self.class_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=2)
                # Gọi hàm cập nhật danh sách lớp khi chọn khoa
                self.department_combobox.bind("<<ComboboxSelected>>", self.update_class_combobox)
        
        else:
            if self.department_frame is not None:
                self.department_frame.destroy()
                self.department_frame = None  # Reset lại frame
                self.selected_department.set("")
                self.selected_class.set("")


    def update_class_combobox(self, event):
        # Lấy mã khoa đã chọn
        selected_department = self.selected_department.get()

        # Cập nhật danh sách lớp tùy theo khoa
        if selected_department == "Cong nghe thong tin":
            classes = list(self.cse_classes.values())
        elif selected_department == "Kinh te":
            classes = list(self.kt_classes.values())
        elif selected_department == "Co khi":
            classes = list(self.ck_classes.values())
        else:
            classes = []

        # Cập nhật Combobox của lớp với danh sách mới
        self.class_combobox['values'] = classes
        self.class_combobox.set('')  # Đặt giá trị rỗng mặc định
    
    def on_closing(self):
        if messagebox.askokcancel("Thoát", "Bạn có chắc chắn muốn thoát không?"):
            self.root.destroy()
            if self.home_callback:
                self.home_callback.destroy()

    def execute_query(self):
        # Xóa bảng cũ nếu có
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()

        # Lấy truy vấn đã chọn từ Combobox
        selected_description = self.selected_query.get()

        # Tìm khóa của truy vấn
        query_key = next((key for key, value in self.queries.items() if value == selected_description), None)

        # Nếu tìm được truy vấn, gọi lớp QueryExecutor để xử lý
        if query_key:
            query_executor = QueryExecutor(self.queries)

            department = self.selected_department.get()  # Lấy giá trị khoa từ Combobox
            class_name = self.selected_class.get()  # Lấy giá trị lớp từ Combobox
            result = query_executor.execute_query(query_key, department, class_name)  # Gọi phương thức từ lớp QueryExecutor
            # Nếu có nhiều tham số hơn thì chỉ cần truyền tham số vào thêm là được (như dưới)
            # result = query_executor.execute_query(query_key, department, class_name, semester, student_id)

            if result:
                # Giả sử `result` trả về là tuple (tiêu đề cột, dữ liệu)
                title_column, data = result

                # Tạo Treeview để hiển thị kết quả
                tree = ttk.Treeview(self.root, columns=title_column, show='headings')

                for col in title_column:
                    tree.heading(col, text=col)
                    tree.column(col, width=80)
                
                for row in data:
                    columns = [pair.split(": ")[1] for pair in row.split(", ")]
                    tree.insert('', tk.END, values=columns)

                # Hiển thị Treeview
                tree.pack(fill=tk.BOTH, expand=True)
            else:
                messagebox.showerror("Lỗi", "Không có kết quả cho truy vấn được chọn.")
        else:
            messagebox.showerror("Lỗi", "Truy vấn không hợp lệ.")

    def display_chart(self):
        # Lấy mô tả truy vấn đã chọn từ Combobox
        selected_description = self.selected_query.get()

        # Tìm khóa tương ứng với truy vấn từ từ điển queries
        query_key = next((key for key, value in self.queries.items() if value == selected_description), None)

        # Nếu tìm được khóa truy vấn
        if query_key:
            # Gọi lớp ChartHandler để hiển thị biểu đồ
            chart_handler = ChartHandler()
            department = self.selected_department.get()  # Lấy giá trị khoa từ Combobox
            class_name = self.selected_class.get()  # Lấy giá trị lớp từ Combobox
            chart_handler.display_chart(query_key, department, class_name)  # Gọi phương thức từ lớp ChartHandler để vẽ biểu đồ
        else:
            messagebox.showerror("Lỗi", "Truy vấn không hợp lệ để vẽ biểu đồ.")


    def exit_query(self):
        self.root.destroy()
        if self.home_callback:
            self.home_callback.deiconify()
