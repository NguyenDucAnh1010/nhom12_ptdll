import tkinter as tk
from tkinter import ttk, messagebox
class ComboBox:
    def __init__(self, master):
        self.master = master
        
    def create_comboBox(self,content):
        self.combobox = ttk.Combobox(self.master, values=content)
        self.combobox.pack(pady=5)
        self.combobox.set("Chọn một tùy chọn")
        return self.combobox
    
    def update_comboBox(self, content):
        self.combobox['values'] = content
        self.combobox.set('Chọn một tùy chọn')
        
    def destroy_comboBox(master):
        for widget in master.winfo_children():
            if isinstance(widget, ttk.Combobox):
                widget.destroy()