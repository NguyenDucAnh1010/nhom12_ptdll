import tkinter as tk
from tkinter import messagebox

def on_button_click(main_window, button_name):
    if button_name == "Thoát":
        messagebox.showinfo("Hành động", "Cảm ơn bạn đã sử dụng chương trình. Chúc bạn 10 điểm.")
        main_window.after(1, main_window.destroy)

    elif button_name == "Truy vấn":
        main_window.withdraw()  # Ẩn giao diện chính

        # Trì hoãn import query để tránh vòng lặp
        import query
        query.query(home_callback=main_window.deiconify)  # Gọi lại home khi thoát khỏi query

    elif button_name == "Import":
        main_window.withdraw()  # Ẩn giao diện chính

        # Trì hoãn import query để tránh vòng lặp
        import import_data
        import_data.query(home_callback=main_window.deiconify)  # Gọi lại home khi thoát khỏi query

def home():
    main_window = tk.Tk()
    main_window.title("Chương trình ứng dụng Spark vào Cassandra")
    main_window.geometry("800x600")
    
    # Ẩn nút phóng to
    main_window.resizable(False, False)

    home_background_image = tk.PhotoImage(file="./image/dhtl.png")
    background_label = tk.Label(main_window, image=home_background_image)
    background_label.place(relwidth=1, relheight=1)

    button_frame = tk.Frame(main_window, bg='')
    button_frame.place(relx=0.5, rely=0.5, anchor='center')

    buttons_info = [
        ("Import", "🎓"),
        ("Truy vấn", "👤"),
        ("Thoát", "✏️")
    ]

    for i, (text, icon) in enumerate(buttons_info):
        btn = tk.Button(
            button_frame, 
            text=f"{icon}\n\n{text}",
            font=("Arial", 14), 
            width=12, height=4,
            command=lambda name=text: on_button_click(main_window, name)
        )
        btn.grid(row=i // 4, column=i % 4, padx=20, pady=20)

    main_window.mainloop()

home()