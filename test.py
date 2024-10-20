import tkinter as tk
from tkinter import filedialog

# Tạo một cửa sổ tkinter ẩn
root = tk.Tk()
root.withdraw()

# Mở hộp thoại chọn file với các loại tệp tin được chỉ định
file_path = filedialog.askopenfilename(
    title="Chọn file", 
    filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")]
)

# Kiểm tra nếu người dùng đã chọn file
if file_path:
    print(f"Đường dẫn tệp tin đã chọn: {file_path}")
else:
    print("Không có tệp tin nào được chọn.")
