import tkinter as tk
from tkinter import ttk, messagebox
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler
import QueryHome.ui.treeView as treeView
from QueryHome.ui.comboBox import ComboBox

class comboBoxQueries9:
    def create(self):
        department = ComboBox(self.comboBox_frame)
        self.comboBoxDepartment = department.create_comboBox(list(self.dictionary_department.values()))
        self.comboBoxDepartment.pack(side=tk.LEFT,padx=5)

        class_de = ComboBox(self.comboBox_frame)
        self.comboBoxClass = class_de.create_comboBox(content=None)
        self.comboBoxClass.pack(side=tk.LEFT,padx=5)
        
        def updateContent(event):
            if(self.comboBoxDepartment.get()=="Cong nghe thong tin"):
                content = list(self.cse_classes.values())
            elif(self.comboBoxDepartment.get()=="Kinh te"):
                content = list(self.kt_classes.values())
            elif(self.comboBoxDepartment.get()=="Co khi"):
                content = list(self.ck_classes.values())
            else:
                content= []
            class_de.update_comboBox(content=content)
        
        self.comboBoxDepartment.bind("<<ComboboxSelected>>",updateContent)
    def getResult(self):
        return [self.comboBoxDepartment.get(),self.comboBoxClass.get()]

class comboBoxQueries8:
    def create(self):
        department = ComboBox(self.comboBox_frame)
        self.comboBoxDepartment = department.create_comboBox(list(self.dictionary_department.values()))
        self.comboBoxDepartment.pack(side=tk.LEFT,padx=5)

        class_de = ComboBox(self.comboBox_frame)
        self.comboBoxClass = class_de.create_comboBox(content=None)
        self.comboBoxClass.pack(side=tk.LEFT,padx=5)
        
        def updateContent(event):
            if(self.comboBoxDepartment.get()=="Cong nghe thong tin"):
                content = list(self.cse_classes.values())
            elif(self.comboBoxDepartment.get()=="Kinh te"):
                content = list(self.kt_classes.values())
            elif(self.comboBoxDepartment.get()=="Co khi"):
                content = list(self.ck_classes.values())
            else:
                content= []
            class_de.update_comboBox(content=content)
        
        self.comboBoxDepartment.bind("<<ComboboxSelected>>",updateContent)
    def getResult(self):
        return [self.comboBoxClass.get()]
