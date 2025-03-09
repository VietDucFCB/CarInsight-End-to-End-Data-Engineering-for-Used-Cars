from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as _sum, regexp_extract

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline: HDFS to PostgreSQL") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. EXTRACT
print("Extracting data from HDFS Data Lake...")
hdfs_root = "/data_lake/raw/cars"
input_path = f"{hdfs_root}/year_*/month_*/day_*/*.json"

try:
    # Đọc dữ liệu mà không áp schema
    df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(input_path)

    print("Inferred data schema:")
    df.printSchema()

    # Cache dữ liệu để tránh lỗi khi chỉ có _corrupt_record
    df_cached = df.cache()

    print("Sample data (5 rows):")
    df_cached.show(5, truncate=False)

    # Kiểm tra file được đọc
    print("Files read from HDFS:")
    input_files = df.inputFiles()
    for file in input_files[:5]:
        print(file)

    # Kiểm tra corrupt records
    if "_corrupt_record" in df.columns:
        corrupt_df = df_cached.filter(col("_corrupt_record").isNotNull())
        corrupt_count = corrupt_df.count()
        if corrupt_count > 0:
            print(f"Found {corrupt_count} corrupt records:")
            corrupt_df.select("_corrupt_record").show(5, truncate=False)
    else:
        print("No corrupt records column found.")

    # Debug: Đọc từng file mẫu
    sample_files = [
        "hdfs://localhost:9000/data_lake/raw/cars/year_2025/month_03/day_02/1_1.json",
        "hdfs://localhost:9000/data_lake/raw/cars/year_2025/month_01/day_12/38_17.json",
        "hdfs://localhost:9000/data_lake/raw/cars/year_2025/month_02/day_02/25_23.json"
    ]
    for file_path in sample_files:
        print(f"\nReading sample file: {file_path}")
        sample_df = spark.read.json(file_path)
        print(f"Schema for {file_path}:")
        sample_df.printSchema()
        print(f"Data for {file_path} (1 row):")
        sample_df.show(1, truncate=False)

    initial_count = df_cached.count()
    print(f"Total number of records before transformation: {initial_count}")

except Exception as e:
    print(f"Error reading data from HDFS: {e}")
    spark.stop()
    exit(1)

# 3. TRANSFORM
print("Transforming data...")
try:
    # Hàm xử lý N/A và Not Available
    def clean_na_values(column):
        return when(
            col(column).isin("N/A", "Not Available"),
            lit(None)
        ).otherwise(col(column))

    # Chọn các cột cần thiết và transform
    df_transformed = df_cached.select(
        clean_na_values("Title").alias("Title"),
        clean_na_values("Cash Price").alias("Cash Price"),
        clean_na_values("Finance Price").alias("Finance Price"),
        clean_na_values("Finance Details").alias("Finance Details"),
        clean_na_values("Exterior").alias("Exterior"),
        clean_na_values("Interior").alias("Interior"),
        clean_na_values("Mileage").alias("Mileage"),
        clean_na_values("Fuel Type").alias("Fuel Type"),
        clean_na_values("MPG").alias("MPG"),
        clean_na_values("Transmission").alias("Transmission"),
        clean_na_values("Drivetrain").alias("Drivetrain"),
        clean_na_values("Engine").alias("Engine"),
        clean_na_values("Location").alias("Location"),
        clean_na_values("Listed Since").alias("Listed Since"),
        col("VIN"),  # Giữ nguyên VIN
        clean_na_values("Stock Number").alias("Stock Number"),
        clean_na_values("Features").alias("Features"),
        regexp_extract(col("Mileage"), r"\((\d+) miles away\)", 1).cast("integer").alias("Mileage_Number")
    )

    # Loại bỏ trùng lặp dựa trên VIN
    df_transformed = df_transformed.dropDuplicates(["VIN"])

    final_count = df_transformed.count()
    print(f"Number of records after deduplication: {final_count}")
    print(f"Number of duplicate records removed: {initial_count - final_count}")

    print("Sample data after transformation (5 rows):")
    df_transformed.show(5, truncate=False)

except Exception as e:
    print(f"Error during transformation: {e}")
    spark.stop()
    exit(1)

# 4. LOAD
print("Loading data to PostgreSQL Data Warehouse...")
try:
    # In 5 dòng trước khi load
    print("Final data before loading to PostgreSQL (5 rows):")
    df_transformed.show(5, truncate=False)

    postgres_url = "jdbc:postgresql://localhost:5432/DataWarehouse"
    properties = {
        "user": "postgres",
        "password": "Kkagiuma2004@",
        "driver": "org.postgresql.Driver"
    }

    df_transformed.write \
        .jdbc(url=postgres_url,
              table="cars_inventory",
              mode="append",
              properties=properties)

    print("Data successfully loaded to PostgreSQL!")
except Exception as e:
    print(f"Error loading data to PostgreSQL: {e}")
    spark.stop()
    exit(1)

# 5. Dừng Spark Session
spark.stop()
print("ETL process completed!")