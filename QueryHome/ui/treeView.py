import tkinter as tk
from tkinter import ttk, messagebox
def create_treeView(master,title_column,data):
    tree = ttk.Treeview(master, columns=title_column, show='headings')
    tree.tag_configure('center', anchor='center')
    for col in title_column:
        tree.heading(col, text=col)
        tree.column(col, width=80)
    for row in data:
        tree.insert('', tk.END, values=row)
    for col in title_column:
        tree.column(col, anchor='center')
    tree.pack(fill=tk.BOTH, expand=True)
    return tree

def destroy_treeView(master):
    for widget in master.winfo_children():
            if isinstance(widget, ttk.Treeview):
                widget.destroy()