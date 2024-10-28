import tkinter as tk
from tkinter import ttk, messagebox
from QueryHome.query.query_executor import QueryExecutor
from QueryHome.chart.chart_handler import ChartHandler
import QueryHome.ui.treeView as treeView
from QueryHome.ui.comboBox import ComboBox

class comboBox_UI:
    def create_department(self):
        department = ComboBox(self.comboBox_frame)
        comboBoxDepartment = department.create_comboBox(list(self.dictionary_department.values()))
        comboBoxDepartment.pack(side=tk.LEFT,padx=5)
        return comboBoxDepartment
    def create_class(self):
        class_de = ComboBox(self.comboBox_frame)
        comboBoxClass = class_de.create_comboBox(content=None)
        comboBoxClass.pack(side=tk.LEFT,padx=5)
        return comboBoxClass
    def create_subject(self):
        subject = ComboBox(self.comboBox_frame)
        comboBoxSubject = subject.create_comboBox(content=self.subjects)
        comboBoxSubject.pack(side=tk.LEFT,padx=5)
        return comboBoxSubject
    def create_term(self):
        term = ComboBox(self.comboBox_frame)
        comboBoxTerm = term.create_comboBox(content=self.term)
        comboBoxTerm.pack(side=tk.LEFT,padx=5)
        return comboBoxTerm

class comboBoxDepartmentTerm:
    def create(self):
        self.comboBoxDepartment = comboBox_UI.create_department(self)
        self.comboBoxTerm = comboBox_UI.create_term(self)
    def getResult(self):
        if(self.comboBoxDepartment.get()=="Chọn một tùy chọn"):
            self.comboBoxDepartment.set("")
        if(self.comboBoxTerm.get()=="Chọn một tùy chọn"):
            self.comboBoxTerm.set("")
        return [self.comboBoxDepartment.get(),self.comboBoxTerm.get()]
    
class comboBoxQueriesSubject:
    def create(self):
        self.comboBoxSubject = comboBox_UI.create_subject(self)
    def getResult(self):
        if(self.comboBoxSubject.get()=="Chọn một tùy chọn"):
            self.comboBoxSubject.set("")
        return [self.comboBoxSubject.get()]
    
class comboBoxQueriesTerm:
    def create(self):
        self.comboBoxTerm = comboBox_UI.create_term(self)
    def getResult(self):
        if(self.comboBoxTerm.get()=="Chọn một tùy chọn"):
            self.comboBoxTerm.set("")
        return [self.comboBoxTerm.get()]
    
class comboBoxQueriesDepartmentClass:
    def create(self):
        self.comboBoxDepartment = comboBox_UI.create_department(self)
        self.comboBoxClass = comboBox_UI.create_class(self)
        
        def updateContent(event):
            if(self.comboBoxDepartment.get()=="Cong nghe thong tin"):
                content = list(self.cse_classes.values())
            elif(self.comboBoxDepartment.get()=="Kinh te"):
                content = list(self.kt_classes.values())
            elif(self.comboBoxDepartment.get()=="Co khi"):
                content = list(self.ck_classes.values())
            else:
                content= []
            self.comboBoxClass['values'] = content
            
        self.comboBoxDepartment.bind("<<ComboboxSelected>>",updateContent)
    def getResult(self):
        return [self.comboBoxDepartment.get(),self.comboBoxClass.get()]

    
    
