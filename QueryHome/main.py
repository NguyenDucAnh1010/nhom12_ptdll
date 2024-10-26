import tkinter as tk
from QueryHome.ui.ui_manager import UIManager
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler

def query(home_callback=None):
    root = tk.Tk()
    
    queries = {
        "querie1": "tính điểm trung bình môn theo từng môn",
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

    query_executor = QueryExecutor(queries)
    chart_handler = ChartHandler()
    
    ui_manager = UIManager(root, queries, dictionary_department, cse_classes, kt_classes, ck_classes, home_callback)

    # Khởi động giao diện
    root.mainloop()
