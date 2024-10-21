import tkinter as tk
from tkinter import ttk
from cassandra.cluster import Cluster  # Cassandra driver for Python
from tkinter import messagebox

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

    # Từ điển queries với khóa và mô tả truy vấn
    queries = ["Department", "Class", "Student", "Subject", "Grade"]
    selected_query = tk.StringVar(root)

    # Tạo PanedWindow chia 2 bên
    paned_window = tk.PanedWindow(root, orient=tk.HORIZONTAL)
    paned_window.pack(fill=tk.BOTH, expand=True)

    # Tạo frame bên trái cho phần nhập dữ liệu
    left_frame = tk.Frame(paned_window, bg='lightgray')
    paned_window.add(left_frame)

    # Tạo Frame cho hàng đầu tiên gồm nút Thoát và Label
    top_left_frame = tk.Frame(left_frame, bg='lightgray')
    top_left_frame.pack(fill=tk.X, padx=10, pady=10)

    # Nút Thoát nằm bên trái Label
    exit_button = tk.Button(top_left_frame, text="Thoát", command=lambda: exit_query())
    exit_button.pack(side=tk.LEFT, padx=5)

    # Label nằm sau nút Thoát, lùi xuống với padding
    tk.Label(top_left_frame, text="Nhập dữ liệu vào bảng:", bg='lightgray').pack(side=tk.LEFT, padx=10, pady=10)

    # Frame cho các Label và Entry động sẽ được sinh sau khi chọn bảng
    dynamic_input_frame = tk.Frame(left_frame, bg='lightgray')
    dynamic_input_frame.pack(fill=tk.X, padx=10, pady=10)  # Lùi form xuống với pady=10

    # Tạo một frame mới cho các nút thêm và xóa
    buttons_frame = tk.Frame(left_frame, bg='lightgray')
    buttons_frame.pack(padx=10, pady=10)

    # Nút thêm dữ liệu nằm trong frame mới
    insert_button = tk.Button(buttons_frame, text="Thêm dữ liệu", command=lambda: insert_data())
    insert_button.pack(side=tk.LEFT, padx=5, pady=5)  # Sử dụng side=tk.LEFT để nằm cạnh nhau

    # Nút xoá dữ liệu nằm cạnh nút thêm trong frame mới
    delete_button = tk.Button(buttons_frame, text="Xoá dữ liệu", command=lambda: delete_data())
    delete_button.pack(side=tk.LEFT, padx=5, pady=5)  # Sử dụng side=tk.LEFT để nằm cạnh nhau

    # Tạo frame bên phải cho combobox và bảng kết quả
    right_frame = tk.Frame(paned_window)
    paned_window.add(right_frame)

    # Tạo Frame chứa Combobox và Button
    query_frame = tk.Frame(right_frame)
    query_frame.pack(fill=tk.X, padx=10, pady=10)

    # Combobox hiển thị mô tả truy vấn
    query_combobox = ttk.Combobox(query_frame, textvariable=selected_query, values=queries)
    query_combobox.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10, pady=10)

    # Nút chạy truy vấn
    run_button = tk.Button(query_frame, text="Xem tất cả dữ liệu", command=lambda: select_query())
    run_button.pack(side=tk.LEFT, padx=5)

    # Kết nối đến Cassandra Cluster
    cluster = Cluster(['127.0.0.1'])  # Thay bằng IP Cassandra nếu khác
    session = cluster.connect('nhom12')  # Thay 'your_keyspace' bằng tên keyspace của bạn

    # Biến toàn cục column_names
    global column_names  # Khai báo column_names là toàn cục
    column_names = []  # Khởi tạo giá trị ban đầu
    global selected_table
    selected_table = ""  # Get the selected table

    # Tạo Treeview hiển thị dữ liệu
    def create_table(column_names, data):
        # Clear old treeview if exists
        for widget in right_frame.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()

        # Create Treeview with column names
        tree = ttk.Treeview(right_frame, columns=column_names, show='headings')
        tree.pack(fill=tk.BOTH, expand=True)

        for col in column_names:
            tree.heading(col, text=col)
            tree.column(col, width=50)  # Giảm độ rộng cột (80 -> 50 hoặc giá trị nhỏ hơn)

        # Insert data into Treeview
        for row in data:
            tree.insert('', tk.END, values=row)


    # Hàm chọn bảng và lấy dữ liệu
    def select_query():
        global column_names  # Khai báo column_names là toàn cục
        global selected_table
        selected_table = selected_query.get()  # Get the selected table
        
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

    # Khai báo biến entry_widgets để chứa các Entry
    entry_widgets = {}

    # Tạo form nhập liệu động dựa trên các cột
    def create_dynamic_inputs(column_names):
        # Clear old inputs if any
        for widget in dynamic_input_frame.winfo_children():
            widget.destroy()

        # Sử dụng grid() để căn chỉnh các Label và Entry thành hàng với nhau
        for index, col in enumerate(column_names):
            input_label = tk.Label(dynamic_input_frame, text=col + ":", bg='lightgray')
            input_label.grid(row=index, column=0, padx=5, pady=5, sticky=tk.W)

            input_entry = tk.Entry(dynamic_input_frame)
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
            
            # Prepare the columns and values for the INSERT query
            columns = ", ".join(new_data.keys())
            values = ", ".join(f"'{value}'" for value in new_data.values())  # Quote each value as Cassandra expects strings

            # Construct the INSERT INTO query
            insert_query = f"INSERT INTO {selected_table} ({columns}) VALUES ({values});"
            
            # Execute the insert query
            session.execute(insert_query)
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