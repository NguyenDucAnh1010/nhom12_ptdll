import threading
import tkinter as tk
from tkinter import ttk
from cassandra.cluster import Cluster  # Cassandra driver for Python
from cassandra.query import SimpleStatement
from tkinter import messagebox
import abcd.import_cmd as import_cmd
import abcd.searchQuery as searchQuery
from dataclass import Department,Student,Subject,Grade,Classes

def query(home_callback=None):
    # Tạo giao diện tkinter
    root = tk.Tk()
    root.title("Ứng dụng Spark vào Cassandra")
    root.geometry("1000x600")
    root.state('zoomed')

    def on_closing():
        # Hiện thông báo xác nhận trước khi đóng
        if messagebox.askokcancel("Thoát", "Bạn có chắc chắn muốn thoát không?"):
            root.destroy() # Đóng cửa sổ
            home_callback.destroy() # Đóng cửa sổ

    root.protocol("WM_DELETE_WINDOW", on_closing)

    # Tạo PanedWindow chia 2 bên
    paned_window = tk.PanedWindow(root, orient=tk.HORIZONTAL)
    paned_window.pack(fill=tk.BOTH, expand=True,padx=30, pady=30)

    # Tạo frame bên trái cho phần nhập dữ liệu
    left_frame = tk.Frame(paned_window)
    paned_window.add(left_frame)

    # Tạo Frame cho hàng đầu tiên gồm nút Thoát và Label
    top_left_frame = tk.Frame(left_frame)
    top_left_frame.pack(fill=tk.X, padx=10, pady=10)

    # Nút Thoát nằm bên trái Label
    exit_button = tk.Button(top_left_frame, text="Thoát", font=("Arial", 14), command=lambda: exit_query())
    exit_button.pack(side=tk.LEFT, padx=5)

    # Label nằm sau nút Thoát, lùi xuống với padding
    tk.Label(top_left_frame, text="Nhập dữ liệu vào bảng:", font=("Arial", 14)).pack(side=tk.LEFT, padx=10, pady=10)

    # Frame cho các Label và Entry động sẽ được sinh sau khi chọn bảng
    CSDL_button_frame = tk.Frame(left_frame)
    CSDL_button_frame.pack(fill=tk.X, padx=10, pady=10)  # Lùi form xuống với pady=10

    # Nút thêm dữ liệu nằm trong frame mới
    delete_CSDL_button = tk.Button(CSDL_button_frame, text="Xoá CSDL", font=("Arial", 14), command=lambda: delete_CSDL())
    delete_CSDL_button.pack(side=tk.LEFT, padx=5, pady=5)

    # Nút thêm dữ liệu nằm trong frame mới
    CSDL_button = tk.Button(CSDL_button_frame, text="Tạo CSDL tự động", font=("Arial", 14), command=lambda: create_CSDL())
    CSDL_button.pack(side=tk.LEFT, padx=5, pady=5)

    # Frame cho các Label và Entry động sẽ được sinh sau khi chọn bảng
    dynamic_input_frame = tk.Frame(left_frame)
    dynamic_input_frame.pack(fill=tk.X, padx=10, pady=10)  # Lùi form xuống với pady=10

    # Tạo một frame mới cho các nút thêm và xóa
    buttons_frame = tk.Frame(left_frame)
    buttons_frame.pack(padx=10, pady=10)

    # Nút thêm dữ liệu nằm trong frame mới
    insert_button = tk.Button(buttons_frame, text="Thêm dữ liệu", font=("Arial", 14), command=lambda: insert_data())
    insert_button.pack(side=tk.LEFT, padx=5, pady=5)  # Sử dụng side=tk.LEFT để nằm cạnh nhau

    # Nút xoá dữ liệu nằm cạnh nút thêm trong frame mới
    delete_button = tk.Button(buttons_frame, text="Xoá dữ liệu", font=("Arial", 14), command=lambda: delete_data())
    delete_button.pack(side=tk.LEFT, padx=5, pady=5)  # Sử dụng side=tk.LEFT để nằm cạnh nhau

    # Tạo frame bên phải cho combobox và bảng kết quả
    right_frame = tk.Frame(paned_window)
    paned_window.add(right_frame)

    # Điều chỉnh chia 2 bên theo tỷ lệ 50:50
    paned_window.paneconfig(left_frame, minsize=500)  # Đặt kích thước tối thiểu bên trái
    paned_window.paneconfig(right_frame, minsize=700)  # Đặt kích thước tối thiểu bên phải

    # Tạo Frame chứa Combobox và Button
    query_frame = tk.Frame(right_frame)
    query_frame.pack(fill=tk.X, padx=10, pady=10)

    # Từ điển queries với khóa và mô tả truy vấn
    queries = ["department", "class", "student", "subject", "grade"]
    selected_query = tk.StringVar(root)

    # Combobox hiển thị mô tả truy vấn
    query_combobox = ttk.Combobox(query_frame, textvariable=selected_query, values=queries, font=("Arial", 14))
    query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

    # Nút chạy truy vấn
    run_button = tk.Button(query_frame, text="Xem tất cả dữ liệu", font=("Arial", 14), command=lambda: select_query())
    run_button.pack(side=tk.LEFT, padx=5)

    # Tạo Frame chứa Combobox, Entry  và Button
    search_frame = tk.Frame(right_frame)
    search_frame.pack(fill=tk.X, padx=10, pady=10)

    # Từ điển queries với khóa và mô tả truy vấn
    searches = ["Cassandra","Spark"]
    selected_search = tk.StringVar(root)

    # Combobox hiển thị lựa chọn loại tìm kiếm
    search_combobox = ttk.Combobox(search_frame, textvariable=selected_search, values=searches, font=("Arial", 14))
    search_combobox.set(searches[0])  # Đặt giá trị mặc định là mô tả của truy vấn đầu tiên
    search_combobox.pack(side=tk.LEFT, fill=tk.X, padx=10, pady=10)

    # Entry hiển thị mô tả truy vấn
    search_input = ttk.Entry(search_frame, font=("Arial", 14))
    search_input.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

    # Nút chạy truy vấn
    search_button = tk.Button(search_frame, text="Tìm kiếm", font=("Arial", 14), command=lambda: search())
    search_button.pack(side=tk.LEFT, padx=5)

    # Kết nối đến Cassandra Cluster
    try:
        cluster = Cluster(['127.0.0.1'])  # Thay bằng IP Cassandra nếu khác
    except Exception as e:
        messagebox.showinfo("Thông báo", f"Đã xảy ra lỗi khi kết nối tới cơ sở dữ liệu:\n{e}")
    global keyspace_exists,session
    keyspace_exists = True
    try:
        session = cluster.connect('nhom12')  # Thay 'your_keyspace' bằng tên keyspace của bạn
    except Exception as e:
        messagebox.showinfo("Thông báo", f"Bạn chưa tạo cơ sở dữ liệu.\nVui lòng ấn vào nút tạo CSDL tự dộng!")
        keyspace_exists = False

    def delete_CSDL():
        global keyspace_exists,session
        if keyspace_exists:
            try:
                cluster = Cluster(['127.0.0.1'])
                session = cluster.connect()  # Kết nối mà không chỉ định keyspace
                session.execute("DROP KEYSPACE nhom12;")
                messagebox.showinfo("Thông báo", f"Đã xoá CSDL thành công!")
                keyspace_exists = False

            except Exception as e:
                messagebox.showinfo("Thông báo", f"Đã xảy ra lỗi khi chạy tệp CQL:\n{e}")             

    def create_CSDL():
        global keyspace_exists,session
        if not keyspace_exists:
            try:
                cluster = Cluster(['127.0.0.1'])
                session = cluster.connect()  # Kết nối mà không chỉ định keyspace
                # Đọc nội dung tệp table.cql và thực thi
                # Đọc tệp và thực hiện từng câu lệnh một
                with open('./table.sql', 'r') as cql_file:
                    cql_script = cql_file.read()
                    # Tách các câu lệnh bằng dấu chấm phẩy
                    commands = cql_script.split(';')
                    
                    for command in commands:
                        command = command.strip()  # Xóa khoảng trắng
                        if command:  # Kiểm tra nếu câu lệnh không rỗng
                            try:
                                session.execute(SimpleStatement(command))  # Thực thi từng câu lệnh
                            except Exception as e:
                                print(f"Lỗi khi thực thi câu lệnh: {command}\n{e}")

                keyspace_exists = True
                # Kết nối lại với keyspace đã tạo
                session = cluster.connect('nhom12')
                # Tạo và khởi động luồng
                threading.Thread(target=import_cmd.run_spark_job()).start()

            except Exception as e:
                messagebox.showinfo("Thông báo", f"Đã xảy ra lỗi khi chạy tệp CQL:\n{e}")
                keyspace_exists = False

    # Biến toàn cục column_names
    global column_names  # Khai báo column_names là toàn cục
    column_names = []  # Khởi tạo giá trị ban đầu
    global selected_table
    selected_table = ""  # Get the selected table

    def search():
        global column_names  # Khai báo column_names là toàn cục
        global selected_table
        search_label = search_input.get()
        search_select = search_combobox.get()

        if selected_table in queries and search_label != "" and search_select != "":
            if search_select == "Cassandra":
                # Prepare and execute query to fetch all rows from the selected table
                query = f"SELECT * FROM {selected_table} WHERE {column_names[0]} = '{search_label}';"
                rows = session.execute(query)

                # Get column names from the metadata
                column_names = rows.column_names
                data = [list(row) for row in rows]

                # Create table with the fetched data
                create_table(column_names, data)
            elif search_select == "Spark":
                # Tạo và khởi động luồng
                create_table_search(search_label)
        else:
            select_query()

    def create_table_search(search_label):
        global selected_table,column_names
        data = searchQuery.run_spark_job(selected_table,column_names[0],search_label)

        # Xóa Treeview cũ nếu có
        for widget in right_frame.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()
            if isinstance(widget, ttk.Scrollbar):
                widget.destroy()

        # Tạo Treeview với tên cột
        tree = ttk.Treeview(right_frame, columns=column_names, show='headings')
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Tạo Scrollbar
        scrollbar = ttk.Scrollbar(right_frame, orient=tk.VERTICAL, command=tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Liên kết scrollbar với treeview
        tree.configure(yscrollcommand=scrollbar.set)

        for col in column_names:
            tree.heading(col, text=col, anchor=tk.CENTER)
            tree.column(col, width=50, anchor=tk.CENTER)

        # Chèn dữ liệu vào Treeview
        for row in data:
            columns = [pair.split(": ")[1] for pair in row.split(", ")]
            tree.insert('', tk.END, values=columns)
        
    # Hàm chọn bảng và lấy dữ liệu
    def select_query():
        global column_names  # Khai báo column_names là toàn cục
        global selected_table
        global keyspace_exists,session
        selected_table = selected_query.get()  # Get the selected table

        if not keyspace_exists:
            try:
                cluster = Cluster(['127.0.0.1'])
                session = cluster.connect('nhom12')  # Thay 'your_keyspace' bằng tên keyspace của bạn
            except Exception as e:
                messagebox.showinfo("Thông báo", f"Bạn chưa tạo cơ sở dữ liệu.\nVui lòng ấn vào nút tạo CSDL tự dộng!")
                keyspace_exists = False
                return
        
        # Prepare and execute query to fetch all rows from the selected table
        query = f"SELECT * FROM {selected_table};"
        rows = session.execute(query)

        # Get column names from the metadata
        column_names = rows.column_names
        data = [list(row) for row in rows]

        # Create table with the fetched data
        create_table(column_names, data)

        # Tạo form nhập dữ liệu động dựa trên các cột của bảng
        create_dynamic_inputs(column_names)

    # Tạo Treeview hiển thị dữ liệu
    def create_table(column_names, data):
        # Xóa Treeview cũ nếu có
        for widget in right_frame.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()
            if isinstance(widget, ttk.Scrollbar):
                widget.destroy()

        # Tạo Treeview với tên cột
        tree = ttk.Treeview(right_frame, columns=column_names, show='headings')
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Tạo Scrollbar
        scrollbar = ttk.Scrollbar(right_frame, orient=tk.VERTICAL, command=tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Liên kết scrollbar với treeview
        tree.configure(yscrollcommand=scrollbar.set)

        for col in column_names:
            tree.heading(col, text=col, anchor=tk.CENTER)
            tree.column(col, width=50, anchor=tk.CENTER)

        # Chèn dữ liệu vào Treeview
        for row in data:
            tree.insert('', tk.END, values=row)
            
    # Khai báo biến entry_widgets để chứa các Entry
    entry_widgets = {}

    # Tạo form nhập liệu động dựa trên các cột
    def create_dynamic_inputs(column_names):
        # Clear old inputs if any
        for widget in dynamic_input_frame.winfo_children():
            widget.destroy()

        # Sử dụng grid() để căn chỉnh các Label và Entry thành hàng với nhau
        for index, col in enumerate(column_names):
            input_label = tk.Label(dynamic_input_frame, text=col + ":", font=("Arial", 14))
            input_label.grid(row=index, column=0, padx=5, pady=5, sticky=tk.W)

            input_entry = tk.Entry(dynamic_input_frame, font=("Arial", 14))
            input_entry.grid(row=index, column=1, padx=5, pady=5, sticky=tk.EW)

            # Lưu lại Entry widget tương ứng với cột
            entry_widgets[col] = input_entry

        # Mở rộng cột thứ 2 (Entry) để chiếm không gian còn lại
        dynamic_input_frame.grid_columnconfigure(1, weight=2)  # Mở rộng form insert

    # Hàm thêm dữ liệu
    def insert_data():
        global selected_table

        try:
            # Gather the input data from the form
            new_data = {col: entry_widgets[col].get() for col in entry_widgets if entry_widgets[col].winfo_exists()}

            # Chuyển đổi giá trị sang loại dữ liệu thích hợp dựa trên lớp dataclass
            if selected_table == 'department':
                new_record = Department(
                    iddepartment=new_data['iddepartment'],
                    namedepartment=new_data['namedepartment']
                )
                values = (new_record.iddepartment, new_record.namedepartment)

            elif selected_table == 'classes':
                new_record = Classes(
                    idclass=new_data['idclass'],
                    nameclass=new_data['nameclass'],
                    iddepartment=new_data['iddepartment']
                )
                values = (new_record.idclass, new_record.iddepartment, new_record.nameclass)

            elif selected_table == 'student':
                new_record = Student(
                    idstudent=new_data['idstudent'],
                    namestudent=new_data['namestudent'],
                    phonenumber=new_data['phonenumber'],
                    address=new_data['address'],
                    idclass=new_data['idclass']
                )
                values = (new_record.idstudent, new_record.address, new_record.idclass, new_record.namestudent, new_record.phonenumber)

            elif selected_table == 'subject':
                new_record = Subject(
                    idsubject=new_data['idsubject'],
                    namesubject=new_data['namesubject'],
                    credit=int(new_data['credit'])  # Chuyển đổi credit thành số nguyên
                )
                values = (new_record.idsubject, new_record.credit, new_record.namesubject)

            elif selected_table == 'grade':
                new_record = Grade(
                    idstudent=new_data['idstudent'],
                    idsubject=new_data['idsubject'],
                    grade=float(new_data['grade']),  # Chuyển đổi grade thành số thực
                    term=int(new_data['term'])
                )
                values = ( new_record.idstudent, new_record.idsubject, new_record.grade, new_record.term)

            # Prepare the columns and values for the INSERT query
            columns = ", ".join(new_data.keys())
            placeholders = ", ".join(['%s'] * len(values))  # Dùng %s để thay thế cho các giá trị

            # Construct the INSERT INTO query
            insert_query = f"INSERT INTO {selected_table} ({columns}) VALUES ({placeholders});"
            
            # Execute the insert query
            session.execute(insert_query, values)  # Gửi values vào execute
            messagebox.showinfo("Thông báo", f"Dữ liệu đã được thêm vào {selected_table}")
            
            # Reload the table to reflect the newly inserted data
            select_query()  # Re-fetch and reload the table data
            
        except Exception as e:
            messagebox.showinfo("Thông báo", f"Đã xảy ra lỗi khi thêm dữ liệu: {e}")

    # Hàm xóa dữ liệu
    def delete_data():
        global selected_table

        # Get the Treeview widget
        tree = next((widget for widget in right_frame.winfo_children() if isinstance(widget, ttk.Treeview)), None)
        
        if tree is None:
            messagebox.showwarning("Cảnh báo", "Không có bảng dữ liệu để xóa.")
            return

        # Get the selected row from the treeview
        selected_item = tree.selection()
        if not selected_item:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn một dòng để xóa.")
            return

        # Assuming the first column is the primary key
        selected_row = tree.item(selected_item)['values']
        primary_key = selected_row[0]  # Adjust according to your primary key column

        try:
            # Construct the DELETE query
            delete_query = f"DELETE FROM {selected_table} WHERE {column_names[0]} = '{primary_key}';"  # Now you can use column_names[0]
            
            # Execute the delete query
            session.execute(delete_query)
            messagebox.showinfo("Thông báo", f"Dữ liệu đã được xóa khỏi {selected_table}")

            # Reload the table to reflect the newly deleted data
            select_query()  # Re-fetch and reload the table data
            
        except Exception as e:
            messagebox.showinfo("Thông báo", f"Đã xảy ra lỗi khi xóa dữ liệu: {e}")

    # Hàm thoát
    def exit_query():
        root.destroy()
        if home_callback:
            home_callback.deiconify()

    root.mainloop()