import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf

# Sử dụng argparse để lấy tham số từ dòng lệnh
parser = argparse.ArgumentParser(description='Process some inputs for Spark job.')
parser.add_argument('--selected_table', required=True, help='Path to the input CSV file')
parser.add_argument('--column_names', required=True, help='Path to the output folder')
parser.add_argument('--search_label', required=True, help='Subject (mon hoc)')
args = parser.parse_args()

# Cấu hình Spark
conf = SparkConf() \
    .setAppName("Search") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.executor.heartbeatInterval", "100s") \
    .set("spark.network.timeout", "600s") \
    .set("spark.rpc.message.maxSize", "1024") \
    .set("spark.cassandra.connection.host", "cassandra-container")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Đọc thông tin từ dòng lệnh
selected_table = args.selected_table
column_names = args.column_names
search_label = args.search_label

# Load the Cassandra table into a DataFrame
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .load(selected_table)

# Filter the DataFrame based on the search criteria
filtered_df = df.filter(df[column_names] == search_label)

# Thu thập kết quả sau khi lọc
results = filtered_df.collect()

# In kết quả với tên cột và giá trị trên cùng một dòng
for row in results:
    row_dict = row.asDict()  # Chuyển đổi hàng thành từ điển
    row_output = ", ".join([f"{col_name}: {row_dict[col_name]}" for col_name in row_dict])  # Kết hợp thành chuỗi
    print(row_output)

# Đóng Spark session
spark.stop()