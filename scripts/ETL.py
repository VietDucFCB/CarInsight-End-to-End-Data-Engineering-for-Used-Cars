from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, when, lit, sum as _sum, regexp_replace, regexp_extract, trim, split
)

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
    # Đọc dữ liệu, chú ý 'multiLine' để đọc file có nhiều dòng JSON
    df = spark.read \
        .option("mergeSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .option("multiLine", "true") \
        .schema(schema) \
        .json(input_path)

    print("Data schema:")
    df.printSchema()

    print("Sample data (5 rows):")
    df.show(5, truncate=False)

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
    # Step 1: Clean "N/A" or "Not Available"
    def clean_na_values(column):
        return when(
            col(column).isin("N/A", "Not Available"),
            lit(None)
        ).otherwise(col(column))

    df_transformed = df.select([
        clean_na_values(c).alias(c) if c != "VIN" else col(c)
        for c in df.columns
    ])

    # Step 2: Drop duplicates based on VIN
    df_transformed = df_transformed.dropDuplicates(["VIN"])

    # Step 3: Extract year, make, and construct a full car title
    #  For example: "2016 Ford F-150 Platinum SuperCrew 5.5' Box 4WD"
    #  -> "Year of Manufacture": "2016"
    #  -> "Car Make": "Ford"
    #  -> "Car Full Title": "2016 Ford F-150 Platinum SuperCrew 5.5' Box 4WD"
    #
    #  If Title is null, these columns become null or empty.

    # Extract year
    df_transformed = df_transformed.withColumn(
        "Year of Manufacture",
        regexp_extract(col("Title"), r"^(\d{4})", 1)
    )

    # Extract the make (the first word after the 4-digit year, if present).
    df_transformed = df_transformed.withColumn(
        "Car Make",
        regexp_extract(col("Title"), r"^\d{4}\s+(\S+)", 1)
    )

    # Generate "Car Full Title" as a String
    df_transformed = df_transformed.withColumn(
        "Car Full Title",
        when(col("Title").isNotNull(), col("Title")).otherwise(None).cast(StringType())
    )

    # Step 4: Clean and rename "Cash Price" -> "Cash Price (USD)"
    # Remove '$' from the column
    df_transformed = df_transformed.withColumnRenamed("Cash Price", "Cash Price (USD)")
    df_transformed = df_transformed.withColumn(
        "Cash Price (USD)",
        trim(regexp_replace(col("Cash Price (USD)"), r"\$", ""))
    )

    # Step 5: Rename and clean "Finance Price" -> "Installment Per Month"
    df_transformed = df_transformed.withColumnRenamed("Finance Price", "Installment Per Month")
    df_transformed = df_transformed.withColumn(
        "Installment Per Month",
        regexp_extract(col("Installment Per Month"), r"(\d+)", 1)
    )

    # Step 6: Split Finance Details -> Down Payment, Loan Term (Months), Interest Rate (APR)
    split_cols = split(col("Finance Details"), "·")

    df_transformed = (
        df_transformed
        .withColumn("chunk1", split_cols.getItem(0))
        .withColumn("chunk2", split_cols.getItem(1))
        .withColumn("chunk3", split_cols.getItem(2))
    )

    # Down Payment (cleaning out '$' and non-digit)
    df_transformed = df_transformed.withColumn(
        "Down Payment",
        trim(regexp_replace(regexp_replace(col("chunk1"), r"^\$", ""), r"[^\d,]", ""))
    )

    # Loan Term (Months)
    df_transformed = df_transformed.withColumn(
        "Loan Term (Months)",
        regexp_extract(col("chunk2"), r"(\d+)", 1)
    )

    # Interest Rate (APR)
    df_transformed = df_transformed.withColumn(
        "Interest Rate (APR)",
        regexp_extract(col("chunk3"), r"(\d+(\.\d+)?)", 1)
    )

    # Remove original Finance Details column and intermediate columns
    df_transformed = df_transformed.drop("Finance Details", "chunk1", "chunk2", "chunk3")

    # Step 7: Clean the Mileage column (e.g. "75,664 miles" -> "75,664")
    df_transformed = df_transformed.withColumn(
        "Mileage",
        trim(regexp_replace(col("Mileage"), r"[^\d,]", ""))
    )

    # Drop original Title column
    df_transformed = df_transformed.drop("Title")

    # Step 8: Reorder the columns
    desired_order = [
        "Year of Manufacture",
        "Car Make",
        "Car Full Title",
        "Cash Price (USD)",
        "Down Payment",
        "Installment Per Month",
        "Loan Term (Months)",
        "Interest Rate (APR)"
    ]
    all_cols = df_transformed.columns
    remaining_cols = [c for c in all_cols if c not in desired_order]
    final_columns = desired_order + remaining_cols

    df_transformed = df_transformed.select(final_columns)

    final_count = df_transformed.count()
    print(f"Number of records after deduplication: {final_count}")
    print(f"Number of duplicate records removed: {initial_count - final_count}")

    print("Sample data after transformation (5 rows):")
    df_transformed.show(5, truncate=False)

except Exception as e:
    print(f"Error during transformation: {e}")
    spark.stop()
    exit(1)

# 5. LOAD
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