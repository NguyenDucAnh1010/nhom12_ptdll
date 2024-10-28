import matplotlib.pyplot as plt
import pandas as pd
class Chart:
    def create(result,title,x_name,y_name,xticklabels=None,yticklabels=None):
        data_lines = result
        print(result)
        if isinstance(data_lines[0], str):  # Nếu chỉ có một tên cột, tạo DataFrame đơn cột
            df = pd.DataFrame(data=data_lines[1:], columns=[data_lines[0]])
        else:
            df = pd.DataFrame(data=data_lines[1:], columns=data_lines[0])        # Vẽ biểu đồ cột
        df = df.apply(pd.to_numeric, errors='ignore')
        ax = df.plot(kind='bar', figsize=(10, 6), width=0.5)

        # Đặt tiêu đề và nhãn trục
        ax.set_title(title, fontsize=14)  # Cập nhật tiêu đề cho đúng nội dung
        ax.set_xlabel(x_name, fontsize=12)  # Cập nhật lại trục x cho phù hợp
        ax.set_ylabel(y_name, fontsize=12)
        
        if(xticklabels!=None):
            ax.set_xticklabels(xticklabels, rotation=0, ha='center') # Đảm bảo cột 'term' tồn tại trong DataFrame
        if(yticklabels!=None):
            ax.set_yticklabels(yticklabels,rotation=0,ha="center")
        # Định dạng trục y thành phần trăm
        # ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.0%}'))

        # Hiển thị biểu đồ
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.show()