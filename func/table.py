import ast
class Table:
    def create(result,schema):
        table_data = []
        headers = schema.fieldNames()
        table_data.append(headers)

        for row in result:
            table_data.append([row[col] for col in headers])
        
        return table_data
    def convert_data(data):
        data_arr = []
        for line in data:
            if line.startswith("["):
                list_line = ast.literal_eval(line)
                data_arr.append(list_line)
        if not data_arr:
            raise ValueError("Không tìm thấy kết quả phù hợp")
        return data_arr