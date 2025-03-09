from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, sum as _sum

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline: HDFS to PostgreSQL") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Định nghĩa schema (khớp với sample JSON bạn cung cấp)
schema = StructType([
    StructField("Title", StringType(), nullable=True),
    StructField("Cash Price", StringType(), nullable=True),
    StructField("Finance Price", StringType(), nullable=True),
    StructField("Finance Details", StringType(), nullable=True),
    StructField("Exterior", StringType(), nullable=True),
    StructField("Interior", StringType(), nullable=True),
    StructField("Mileage", StringType(), nullable=True),
    StructField("Fuel Type", StringType(), nullable=True),
    StructField("MPG", StringType(), nullable=True),
    StructField("Transmission", StringType(), nullable=True),
    StructField("Drivetrain", StringType(), nullable=True),
    StructField("Engine", StringType(), nullable=True),
    StructField("Location", StringType(), nullable=True),
    StructField("Listed Since", StringType(), nullable=True),
    StructField("VIN", StringType(), nullable=True),
    StructField("Stock Number", StringType(), nullable=True),
    StructField("Features", StringType(), nullable=True)
])

# 3. EXTRACT
print("Extracting data from HDFS Data Lake...")
hdfs_root = "/data_lake/raw/cars"
input_path = f"{hdfs_root}/year_*/month_*/day_*/*.json"

try:
    # Đọc dữ liệu với các option debug
    df = spark.read \
        .option("mergeSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema) \
        .json(input_path)

    print("Data schema:")
    df.printSchema()

    print("Sample data (5 rows):")
    df.show(5, truncate=False)

    # Kiểm tra record bị corrupt
    corrupt_df = df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()
    if corrupt_count > 0:
        print(f"Found {corrupt_count} corrupt records:")
        corrupt_df.select("_corrupt_record").show(5, truncate=False)

    # Kiểm tra danh sách file được đọc
    print("Files read from HDFS:")
    input_files = df.inputFiles()
    for file in input_files[:5]:  # Giới hạn 5 file để debug
        print(file)

    initial_count = df.count()
    print(f"Total number of records before transformation: {initial_count}")

    # Kiểm tra xem có dữ liệu nào không NULL không
    print("Non-null counts per column:")
    df.select([_sum(col(c).isNotNull().cast("int")).alias(c) for c in df.columns]).show()

except Exception as e:
    print(f"Error reading data from HDFS: {e}")
    # Debug HDFS
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_root)
        files = fs.listStatus(path)
        print("Files in HDFS directory:")
        for f in files:
            print(f.getPath().toString())
    except Exception as hdfs_e:
        print(f"HDFS access error: {hdfs_e}")
    spark.stop()
    exit(1)

# 4. TRANSFORM
print("Transforming data...")
try:
    def clean_na_values(column):
        return when(
            col(column).isin("N/A", "Not Available"),
            lit(None)
        ).otherwise(col(column))

    df_transformed = df.select([
        clean_na_values(c).alias(c) if c != "VIN" else col(c)
        for c in df.columns
    ])

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

# 5. LOAD (giữ nguyên phần này nếu cần)
print("Loading data to PostgreSQL Data Warehouse...")
try:
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

# 6. Dừng Spark Session
spark.stop()
print("ETL process completed!")