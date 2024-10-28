def roundData(data):
    rounded_data = []
    for row in data:
        rounded_numbers = [
            round(num, 2) if isinstance(num, (int, float)) else num for num in row
        ]
        rounded_data.append(rounded_numbers)
    return rounded_data