import tkinter as tk
from QueryHome.ui.ui_manager import UIManager
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler

def query(home_callback=None):
    root = tk.Tk()
    
    queries = {
        "querie1": "tính điểm trung bình môn theo từng môn",
        "querie3": "Top 10 sinh viên có điểm cao nhất theo từng môn",
        "querie4": "Tổng số môn sinh viên đã qua theo từng kỳ",
        "querie9": "thống kê số sinh viên trượt môn (< 4)"
    }

        #Từ điển Khoa với mã khoa và tên khoa
    dictionary_department = {
        "CSE": "Cong nghe thong tin",
        "KT": "Kinh te",
        "CK": "Co khi"
    }

    # Tu diem lop voi ma lop va ten lop
    # Từ điển lớp cho từng khoa
    cse_classes = {
        'HT1': '63HT1',
        'HT2': '63HT2',
        'HT3': '63HT3',
        'HT4': '63HT4',
        'HT5': "63HT5"
    }

    kt_classes = {
        'KT1': '63KT1',
        'KT2': '63KT2',
        'KT3': '63KT3',
        'KT4': '63KT4',
        'KT5': "63KT5"
    }

    ck_classes = {
        'CK1': '63CK1',
        'CK2': '63CK2',
        'CK3': '63CK3',
        'CK4': '63CK4',
        'CK5': "63CK5"
    }

    subjects = ["Co so du lieu", "Co hoc chat long", "Kinh doanh thuong mai", "Tai chinh quoc te", 
                "Khai pha du lieu", "Ke toan", "Bao mat thong tin", "Co so thiet ke may",
                "Thiet ke may", "Khi nen", "Kinh te so", "Ky thuat dien tu", 
                "Quan tri Marketing", "Xu ly anh", "Dung cu va do luong", 
                "Hoc sau", "Phap luat kinh te", "Thanh toan quoc te", "Suc ben vat lieu",
                "Nhap mon Cong nghe thong tin", "Cau truc du lieu va giai thuat", 
                "Nguyen ly ke toan", "Ky thuat che tao", "Phat trien game", 
                "Phat trien phan mem", "May nang", "Cat got kim loai", "Ky thuat nhiet", 
                "Quan tri tac nghiep", "To chuc ke toan", "Xu ly am thanh", 
                "Phan tich du lieu lon", "Tai chinh tien te", "Vat ly co", "Thuy luc", 
                "Hoc may", "Lap trinh huong doi tuong", "Kinh te vi mo", "Tri tue nhan tao", 
                "Vi dieu khien", "Nguyen ly kinh te vi mo", "Quan tri hoc", "Ke toan may", 
                "Ky thuat khuon dap", "Tri tue nhan tao", "Ke toan chi phi", "Co hoc", 
                "Mang may tinh"]
    term = [
        1,
        2
    ]

    query_executor = QueryExecutor(queries)
    chart_handler = ChartHandler()
    
    ui_manager = UIManager(root, queries, dictionary_department, cse_classes, kt_classes, ck_classes, subjects, term, home_callback)

    # Khởi động giao diện
    root.mainloop()
