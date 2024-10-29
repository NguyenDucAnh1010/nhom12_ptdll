import tkinter as tk
from tkinter import ttk, messagebox
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler
import QueryHome.ui.treeView as treeView
from QueryHome.ui.comboBox import ComboBox
from QueryHome.ui.ComboBoxComponent.query import comboBoxQueriesDepartmentClass,comboBoxDepartmentTerm,comboBoxQueriesSubject,comboBoxQueriesTerm, comboBoxQueriesDepartment
from func.chart import Chart
import numpy as np

class UIManager:
    def __init__(self, root, queries, dictionary_department, cse_classes, kt_classes, ck_classes,term,subjects, home_callback=None):
        self.root = root
        self.root.title("Ứng dụng Spark vào Cassandra")
        self.root.geometry("800x600")
        self.queries = queries
        self.home_callback = home_callback


        # Tạo biến chuỗi cho các Combobox
        self.selected_query = tk.StringVar(root)
        self.selected_department = tk.StringVar(root)
        self.selected_term = tk.IntVar(root)  
        self.selected_class = tk.StringVar(root)
        self.dictionary_department = dictionary_department
        self.selected_subject = tk.StringVar(root)  
        self.dictionary_department = dictionary_department  

        # Các từ điển lớp
        self.subjects = subjects  
        self.selected_subject = tk.StringVar(root)
        self.term = term 
        self.selected_term = tk.StringVar(root) 

        # Các từ điển lớp
        self.cse_classes = cse_classes
        self.kt_classes = kt_classes
        self.ck_classes = ck_classes
        self.term= term
        self.subjects = subjects


        # Thiết lập các thành phần giao diện
        self.setup_ui(queries)
        self.comboBox_frame = tk.Frame(self.root)
        self.comboBox_frame.pack(fill=tk.X, padx=65, pady=0)

        # Xử lý sự kiện đóng
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def setup_ui(self, queries):
        # Frames và button
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
        self.classify_frame = None 
        self.scholarship_frame = None 
        self.subject_frame = None # mai
        self.term_frame = None #mai

        #Bat su kien khi hien thi cac combobox khoa va lop
        self.selected_query.trace("w", self.show_combobox)
    # def remove_combox(self):
    #     if self.department_frame is not None:
    #         self.department_frame.destroy()
    #         self.department_frame = None  # Reset lại frame
    #         self.selected_department.set("")
    #         self.selected_class.set("")
    #     if self.subject_frame is not None:
    #         self.subject_frame.destroy()
    #         self.subject_frame = None  # Reset lại frame
    #         self.selected_subject.set("")
    #     if self.term_frame is not None:
    #         self.term_frame.destroy()
    #         self.term_frame = None  # Reset lại frame
    #         self.selected_term.set("")

    def show_combobox(self, *args):
        treeView.destroy_treeView(self.root)
        ComboBox.destroy_comboBox(self.comboBox_frame)

    # Hiển thị combobox dựa trên truy vấn đã chọn
        if self.selected_query.get() == "Danh sách học bổng (điểm >4 và số tín >12)":
            comboBoxDepartmentTerm.create(self)
        elif self.selected_query.get() == "Phân loại sinh viên dựa vào điểm trung bình theo từng môn":
            comboBoxQueriesSubject.create(self)
        elif self.selected_query.get() == "Top 10 sinh viên có điểm cao nhất theo từng môn":
            comboBoxQueriesSubject.create(self)
        elif self.selected_query.get() == "Tổng số môn sinh viên đã qua theo từng kỳ":
            comboBoxQueriesTerm.create(self)
        elif self.selected_query.get() == "Phân tích sự ảnh hưởng của số tín chỉ lên điểm trung bình của sinh viên":
            None
        elif self.selected_query.get() == "Tính điểm trung bình của sinh viên theo từng lớp":
            comboBoxQueriesDepartmentClass.create(self)
        elif self.selected_query.get() == "Thống kê số sinh viên trượt môn":
            comboBoxQueriesDepartmentClass.create(self)
        elif self.selected_query.get() == "Dự đoán sinh viên có khả năng bị cảnh báo học vụ dựa trên phân tích hành vi học tập qua 3 kỳ học gần nhất":
            comboBoxQueriesDepartment.create(self)

    # def update_class_combobox(self, event):
    #     # Lấy mã khoa đã chọn
    #     selected_department = self.selected_department.get()

    #     # Cập nhật danh sách lớp tùy theo khoa
    #     if selected_department == "Cong nghe thong tin":
    #         classes = list(self.cse_classes.values())
    #     elif selected_department == "Kinh te":
    #         classes = list(self.kt_classes.values())
    #     elif selected_department == "Co khi":
    #         classes = list(self.ck_classes.values())
    #     else:
    #         classes = []

    #     # Cập nhật Combobox của lớp với danh sách mới
    #     self.class_combobox['values'] = classes
    #     self.class_combobox.set('')  # Đặt giá trị rỗng mặc định
    
    def on_closing(self):
        if messagebox.askokcancel("Thoát", "Bạn có chắc chắn muốn thoát không?"):
            self.root.destroy()
            if self.home_callback:
                self.home_callback.destroy()

    def execute_query(self):
        treeView.destroy_treeView(self.root)
        
        # Lấy truy vấn đã chọn từ Combobox
        selected_description = self.selected_query.get()

        # Tìm khóa của truy vấn
        query_key = next((key for key, value in self.queries.items() if value == selected_description), None)

        # Nếu tìm được truy vấn, gọi lớp QueryExecutor để xử lý
        if query_key:
            query_executor = QueryExecutor(self.queries)

# Lấy giá trị lớp từ Combobox
            if(query_key=="querie1"):
                self.result_selected = comboBoxDepartmentTerm.getResult(self)
                result = query_executor.execute_query(query_key, name_department=self.result_selected[0],selected_term=self.result_selected[1])  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie2"):
                self.result_selected = comboBoxQueriesSubject.getResult(self)
                result = query_executor.execute_query(query_key, selected_subject=self.result_selected[0])  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie3"):
                self.result_selected = comboBoxQueriesSubject.getResult(self)
                result = query_executor.execute_query(query_key, subject_name=self.result_selected[0])  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie4"):
                self.result_selected = comboBoxQueriesTerm.getResult(self)
                result = query_executor.execute_query(query_key, term=self.result_selected[0])  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie5"):
                result = query_executor.execute_query(query_key)  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie6"):
                self.result_selected = comboBoxQueriesDepartmentClass.getResult(self)
                result = query_executor.execute_query(query_key, selected_class=self.result_selected[1])  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie7"):
                self.result_selected = comboBoxQueriesDepartmentClass.getResult(self)
                class_name = self.result_selected[1]
                if class_name == 'Chọn một tùy chọn':
                    class_name = ''
                result = query_executor.execute_query(query_key, department=self.result_selected[0],class_name=class_name)  # Gọi phương thức từ lớp QueryExecutor
            elif(query_key=="querie8"):
                self.result_selected = comboBoxQueriesDepartment.getResult(self)
                result = query_executor.execute_query(query_key, name_department=self.result_selected[0])
            else:
                result=None
                
            # Nếu có nhiều tham số hơn thì chỉ cần truyền tham số vào thêm là được (như dưới)
            # result = query_executor.execute_query(query_key, department, class_name, semester, student_id)

            # if result and isinstance(result,list):
            #     department = self.selected_department.get()  
            #     class_name = self.selected_class.get()  # Lấy giá trị lớp từ Combobox
            
            # Xử lý truy vấn cho từng trường hợp
            # if query_key == "querie5":  # Phân loại sinh viên
            #     subject_name = self.selected_subject.get()
            #     result = query_executor.execute_query(query_key, subject_name)
            # elif query_key == "querie6":  # Danh sách học bổng
            #     department = self.selected_department.get()  
            #     term = self.selected_term.get()
            #     result = query_executor.execute_query(query_key, department,term)
            # else:  # Các truy vấn khác
            #     result = query_executor.execute_query(query_key, department, class_name)

            # if result:  # Kiểm tra nếu có kết quả trả về
            #     if query_key == "querie9":  # Truy vấn "thống kê số sinh viên trượt môn (< 4)"
            #         department = self.selected_department.get()
            #         class_name = self.selected_class.get()
            #         result = query_executor.execute_query(query_key, department, class_name)

            # elif query_key == "querie3":  # Truy vấn "Top 10 sinh viên có điểm cao nhất theo từng môn"
            #     subject_name = self.selected_subject.get()
            #     result = query_executor.execute_query(query_key, subject_name)
            
            # elif query_key == "querie4":
            #     try:
            #         term_name = int(self.selected_term.get())  # Chuyển term về kiểu int
            #         result = query_executor.execute_query(query_key, term_name)
            #     except ValueError:
            #         messagebox.showerror("Lỗi", "Vui lòng chọn một kỳ hợp lệ")
            #         return
            if result:
                # Giả sử `result` trả về là tuple (tiêu đề cột, dữ liệu)
                title_column = result[0]
                data = result[1:]

                # Tạo Treeview để hiển thị kết quả
                treeView.create_treeView(master=self.root,title_column=title_column,data=data)
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
            chart_handler = ChartHandler()
            # Gọi lớp ChartHandler để hiển thị biểu đồ
            if(query_key=="querie5"):
                result = chart_handler.get_data(query_key)  # Gọi phương thức từ lớp ChartHandler để vẽ biểu đồ
                x_name = ""
                y_name = "Tỉ lệ điểm trung bình"
                title ="Tỉ lệ tín chỉ ảnh hưởng tới chất lượng điểm trung bình"
                xticklabels=None
                yticklabels=None
            elif(query_key=="querie7"):
                self.result_selected = comboBoxQueriesDepartmentClass.getResult(self)
                class_name = self.result_selected[1]
                if class_name == 'Chọn một tùy chọn':
                    class_name = ''
                result = chart_handler.get_data(query_key,department=self.result_selected[0],class_name=class_name)  # Gọi phương thức từ lớp ChartHandler để vẽ biểu đồ
                result_matrix = np.array(result)
                last_column = result_matrix[:, -1]
                term_column = result_matrix[1:,2]
                if(class_name != ''):
                    title = f"Tỉ lệ sinh viên trượt môn lớp {self.result_selected[1]}"
                else:
                    term_column = result_matrix[1:,1]
                    title = f"Tỉ lệ sinh viên trượt môn khoa {self.result_selected[0]}"
                result = last_column.tolist()
                x_name = "Kỳ học"
                y_name = "Tỉ lệ % trượt môn"
                xticklabels= term_column.tolist()
                yticklabels=None
                
            Chart.create(result=result,title = title,x_name=x_name,y_name=y_name,xticklabels=xticklabels,yticklabels=yticklabels)
            # department = self.selected_department.get()  # Lấy giá trị khoa từ Combobox
            # class_name = self.selected_class.get()  # Lấy giá trị lớp từ Combobox
            # namesubject = self.selected_subject.get()
            # chart_handler.display_chart(query_key, department, class_name)  # Gọi phương thức từ lớp ChartHandler để vẽ biểu đồ
    

        else:
            messagebox.showerror("Lỗi", "Truy vấn không hợp lệ để vẽ biểu đồ.")

    def exit_query(self):
        self.root.destroy()
        if self.home_callback:
            self.home_callback.deiconify()
            
