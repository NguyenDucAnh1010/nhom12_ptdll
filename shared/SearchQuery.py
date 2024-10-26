import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

# Sử dụng argparse để lấy tham số từ dòng lệnh
parser = argparse.ArgumentParser(description='Process some inputs for Spark job.')
parser.add_argument('--selected_table', required=True, help='Path to the input CSV file')
parser.add_argument('--search_label', required=True, help='Search label for filtering')
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
search_label = args.search_label

# Load the Cassandra table into a DataFrame
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=selected_table, keyspace='nhom12') \
    .load()

# Lấy danh sách các cột trong DataFrame
columns = df.columns

# Tạo điều kiện tìm kiếm trên toàn bộ cột
filter_condition = F.lit(False)  # Điều kiện ban đầu là False
for col in columns:
    filter_condition = filter_condition | (F.col(col).cast("string").contains(search_label))

# Áp dụng điều kiện lọc cho DataFrame
filtered_df = df.filter(filter_condition)

# Thu thập kết quả sau khi lọc
results = filtered_df.collect()

# In kết quả với tên cột và giá trị trên cùng một dòng
for row in results:
    row_dict = row.asDict()  # Chuyển đổi hàng thành từ điển
    row_output = ", ".join([f"{col_name}: {row_dict[col_name]}" for col_name in row_dict])  # Kết hợp thành chuỗi
    print(row_output)

# Đóng Spark session
spark.stop()