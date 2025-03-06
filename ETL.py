from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Extract with Schema from Data Lake") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Định nghĩa schema dựa trên cấu trúc JSON
schema = StructType([
    StructField("Title", StringType(), True),
    StructField("Cash_Price", StringType(), True),
    StructField("Finance_Price", StringType(), True),
    StructField("Finance_Details", StringType(), True),
    StructField("Exterior", StringType(), True),
    StructField("Interior", StringType(), True),
    StructField("Mileage", StringType(), True),
    StructField("Fuel_Type", StringType(), True),
    StructField("MPG", StringType(), True),
    StructField("Transmission", StringType(), True),
    StructField("Drivetrain", StringType(), True),
    StructField("Engine", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Listed_Since", StringType(), True),
    StructField("VIN", StringType(), True),
    StructField("Stock_Number", StringType(), True),
    StructField("Features", StringType(), True)
])

# 3. Định nghĩa đường dẫn HDFS
hdfs_root = "/data_lake/raw/cars"
input_path = f"{hdfs_root}/year_*/month_*/day_*/*.json"

# 4. Extract: Đọc dữ liệu JSON với schema
print("Extracting data from HDFS Data Lake with schema...")
try:
    df = spark.read.schema(schema).json(input_path)
    print("Data schema:")
    df.printSchema()  # In schema để kiểm tra
    print("Sample data (5 rows):")
    df.show(5, truncate=False)  # Hiển thị 5 dòng, không cắt ngắn
    print(f"Total number of records: {df.count()}")  # Đếm tổng số bản ghi
except Exception as e:
    print(f"Error reading data from HDFS: {e}")
    spark.stop()
    exit(1)

# 5. (Tùy chọn) Lưu vào Hive Metastore
print("Saving to Hive table...")
df.write \
    .mode("overwrite") \
    .saveAsTable("cars_raw")  # Lưu thành bảng Hive

# 6. Dừng Spark Session
spark.stop()
print("Extraction and Hive storage completed!")