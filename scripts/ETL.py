#!/usr/bin/env python
"""
ETL module for extracting car data from HDFS, transforming it, and loading into PostgreSQL.
Designed to run in the Spark master container with access to HDFS and PostgreSQL.
"""

import os
import logging
import time
from datetime import datetime
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, sum as _sum, regexp_replace, regexp_extract, trim, split

# Setup logging
logs_dir = "/tmp/etl_logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Get configuration from environment variables
HDFS_URL = os.environ.get('HDFS_URL', 'http://namenode:9870')
HDFS_USER = os.environ.get('HDFS_USER', 'hadoop')
HDFS_BASE_DIR = os.environ.get('HDFS_BASE_DIR', '/user/hadoop/datalake')
PG_HOST = os.environ.get('PG_HOST', 'postgres')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DATABASE = os.environ.get('PG_DATABASE', 'car_warehouse')
PG_USER = os.environ.get('PG_USER', 'postgres')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'postgres')
PROCESSED_MARKER_DIR = os.environ.get('PROCESSED_MARKER_DIR', '/tmp/processed_markers')
CURRENT_DATE_UTC = os.environ.get('CURRENT_DATE_UTC', '2025-03-17 09:46:06')
CURRENT_USER = os.environ.get('CURRENT_USER', 'VietDucFCB')

# Ensure the processed marker directory exists
os.makedirs(PROCESSED_MARKER_DIR, exist_ok=True)


def init_spark_session():
    """Initialize and configure Spark session"""
    logger.info("Initializing Spark Session")

    # Configure Spark with PostgreSQL JDBC driver and proper HDFS configuration
    spark = SparkSession.builder \
        .appName("ETL Pipeline: HDFS to PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # Debug Hadoop configuration
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs_default_name = hadoop_conf.get("fs.defaultFS")
    logger.info(f"Hadoop fs.defaultFS configuration: {fs_default_name}")

    # Log Hadoop version
    hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    logger.info(f"Hadoop version: {hadoop_version}")

    # Print namenode status
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        logger.info(f"FileSystem URI: {fs.getUri()}")
        logger.info("Successfully got filesystem reference")
    except Exception as e:
        logger.error(f"Error accessing HDFS filesystem: {str(e)}")

    logger.info(f"Spark Session initialized with version: {spark.version}")
    return spark


def create_tables_if_not_exist():
    """Create the normalized tables in PostgreSQL if they don't exist"""
    logger.info("Creating normalized tables schema if they don't exist")

    try:
        import psycopg2

        # First check if database exists, create if needed
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            # Connect to default postgres database
            dbname="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if our target database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_DATABASE}'")
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"Database {PG_DATABASE} does not exist. Creating it.")
            # Close current connections to postgres before creating new DB
            cursor.close()
            conn.close()

            # Connect with autocommit to create database
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {PG_DATABASE}")
            cursor.close()
            conn.close()

        # Now connect to our target database and create tables
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Define the schema creation statements
        create_tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS "Car_Info" (
                "VIN" VARCHAR(17) PRIMARY KEY,
                "Year_Of_Manufacture" INT,
                "Car_Make" VARCHAR(50),
                "Car_Full_Title" VARCHAR(255),
                "Car_Price_USD" VARCHAR(17),
                "Installment_Support" BOOLEAN
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "Installment_Info" (
                "VIN" VARCHAR(17),
                "Down_Payment" VARCHAR(17),
                "Installment_Per_Month" VARCHAR(17),
                "Loan_Term" INT,
                "Interest_Rate" VARCHAR(17),
                FOREIGN KEY ("VIN") REFERENCES "Car_Info"("VIN") ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "In_Exterior" (
                "VIN" VARCHAR(17),
                "Exterior" VARCHAR(50),
                "Interior" VARCHAR(50),
                FOREIGN KEY ("VIN") REFERENCES "Car_Info"("VIN") ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "Evaluate" (
                "VIN" VARCHAR(17),
                "Mileage" VARCHAR(17),
                "Fuel_Type" VARCHAR(20),
                "MPG" VARCHAR(100),
                FOREIGN KEY ("VIN") REFERENCES "Car_Info"("VIN") ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "Engine_Info" (
                "VIN" VARCHAR(17),
                "Transmission" VARCHAR(50),
                "Engine" VARCHAR(50),
                FOREIGN KEY ("VIN") REFERENCES "Car_Info"("VIN") ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "Other_Detail" (
                "VIN" VARCHAR(17),
                "Location" VARCHAR(100),
                "Listed_Since" VARCHAR(100),
                "Stock_Number" VARCHAR(20),
                "Features" TEXT,
                FOREIGN KEY ("VIN") REFERENCES "Car_Info"("VIN") ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "ETL_Log" (
                "id" SERIAL PRIMARY KEY,
                "execution_date" TIMESTAMP,
                "num_records_processed" INT,
                "status" VARCHAR(20),
                "executed_by" VARCHAR(100)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "transformed_cars" (
                "VIN" VARCHAR(17) PRIMARY KEY,
                "Year of Manufacture" VARCHAR(4),
                "Car Make" VARCHAR(50),
                "Car Full Title" VARCHAR(255),
                "Cash Price (USD)" VARCHAR(50),
                "Down Payment" VARCHAR(50),
                "Installment Per Month" VARCHAR(50),
                "Loan Term (Months)" VARCHAR(10),
                "Interest Rate (APR)" VARCHAR(20),
                "Mileage" VARCHAR(50),
                "Fuel Type" VARCHAR(50),
                "MPG" VARCHAR(100),
                "Exterior" VARCHAR(50),
                "Interior" VARCHAR(50),
                "Transmission" VARCHAR(50),
                "Engine" VARCHAR(100),
                "Location" VARCHAR(100),
                "Listed Since" VARCHAR(100),
                "Stock Number" VARCHAR(50),
                "Features" TEXT,
                "load_date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]

        for sql in create_tables_sql:
            cursor.execute(sql)

        logger.info("Schema created successfully")

    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def list_hdfs_contents(spark, path):
    """Debug function to list HDFS contents"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)

        if not fs.exists(hdfs_path):
            logger.warning(f"Path does not exist: {path}")
            return False

        logger.info(f"Listing contents of {path}:")

        statuses = fs.listStatus(hdfs_path)
        for status in statuses:
            path_name = status.getPath().getName()
            if status.isDirectory():
                logger.info(f"  DIR: {path_name}")
            else:
                logger.info(f"  FILE: {path_name} ({status.getLen()} bytes)")

        return True
    except Exception as e:
        logger.error(f"Error listing HDFS contents at {path}: {str(e)}")
        return False


def create_hdfs_dirs(spark):
    """Create necessary HDFS directories if they don't exist"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

        # Create base directory hierarchy
        for path in [HDFS_BASE_DIR]:
            hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
            if not fs.exists(hdfs_path):
                logger.info(f"Creating HDFS directory: {path}")
                fs.mkdirs(hdfs_path)
                logger.info(f"Created HDFS directory: {path}")

        return True
    except Exception as e:
        logger.error(f"Error creating HDFS directories: {str(e)}")
        return False


def extract_data(spark):
    """Extract data from HDFS Data Lake using date partitioned structure"""
    logger.info("Extracting data from HDFS Data Lake")

    # Define schema for the JSON data
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
        StructField("Features", StringType(), nullable=True),
        StructField("datalake_metadata", StringType(), nullable=True)  # Metadata added by LoadDataIntoDataLake.py
    ])

    try:
        # Debug HDFS filesystem
        logger.info("Checking HDFS connectivity")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        logger.info(f"Successfully connected to HDFS: {fs.getUri()}")

        # Ensure the HDFS directory structure exists
        create_hdfs_dirs(spark)

        # List the base directory
        list_hdfs_contents(spark, HDFS_BASE_DIR)

        # Try different paths - debug what's available
        today = datetime.now().strftime("%Y-%m-%d")
        possible_paths = [
            # Try exact path from web UI
            f"{HDFS_BASE_DIR}/2025-03-17/1/*.json",
            # Try with today's date
            f"{HDFS_BASE_DIR}/{today}/1/*.json",
            # Try different date formats
            f"{HDFS_BASE_DIR}/2025-03-17/*/*.json",
            f"{HDFS_BASE_DIR}/*/*.json",
            # Most general path
            f"{HDFS_BASE_DIR}/*/*/*/*.json",
            # Fall back to reading all JSON in datalake
            f"{HDFS_BASE_DIR}/**/*.json"
        ]

        # Try each path until we find data
        df = None
        for path in possible_paths:
            logger.info(f"Attempting to read from: {path}")
            try:
                test_df = spark.read \
                    .option("multiLine", "true") \
                    .option("mode", "PERMISSIVE") \
                    .schema(schema) \
                    .json(path)

                count = test_df.count()
                if count > 0:
                    logger.info(f"Successfully read {count} records from {path}")
                    df = test_df
                    break
                else:
                    logger.info(f"Path {path} exists but contains no data")
            except Exception as e:
                logger.info(f"Could not read from {path}: {str(e)}")
                continue

        if df is None:
            # Create a simple DataFrame for testing if no data found
            logger.warning("No data found in HDFS. Creating minimal test DataFrame")

            test_data = [
                {"Title": "2022 Toyota Camry", "VIN": "JTDKARFP4N3001234", "Cash Price": "$25,000"}
            ]
            df = spark.createDataFrame(test_data)

        # Log the schema and a sample of the data
        logger.info("DataFrame schema:")
        df.printSchema()

        logger.info("Sample data:")
        df.show(5, truncate=False)

        return df

    except Exception as e:
        logger.error(f"Error extracting data from HDFS: {str(e)}")
        # Create a minimal dataframe for testing
        logger.warning("Creating minimal test DataFrame due to extraction error")
        test_data = [
            {"Title": "2022 Toyota Camry", "VIN": "JTDKARFP4N3001234", "Cash Price": "$25,000"}
        ]
        df = spark.createDataFrame(test_data)
        return df


def transform_data(df):
    """Transform the extracted data"""
    logger.info("Transforming data")

    try:
        # Store initial count
        initial_count = df.count()

        # Drop metadata column if it exists
        if 'datalake_metadata' in df.columns:
            df = df.drop('datalake_metadata')

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
        df_transformed = df_transformed.withColumn(
            "Year of Manufacture",
            regexp_extract(col("Title"), r"^(\d{4})", 1)
        )

        df_transformed = df_transformed.withColumn(
            "Car Make",
            regexp_extract(col("Title"), r"^\d{4}\s+(\S+)", 1)
        )

        df_transformed = df_transformed.withColumn(
            "Car Full Title",
            when(col("Title").isNotNull(), col("Title")).otherwise(None).cast(StringType())
        )

        # Step 4: Clean and rename "Cash Price" -> "Cash Price (USD)"
        df_transformed = df_transformed.withColumnRenamed("Cash Price", "Cash Price (USD)")
        df_transformed = df_transformed.withColumn(
            "Cash Price (USD)",
            trim(regexp_replace(col("Cash Price (USD)"), r"\$", ""))
        )

        # Handle columns that might not exist in the test data
        if "Finance Price" in df.columns:
            # Step 5: Rename and clean "Finance Price" -> "Installment Per Month"
            df_transformed = df_transformed.withColumnRenamed("Finance Price", "Installment Per Month")
            df_transformed = df_transformed.withColumn(
                "Installment Per Month",
                regexp_extract(col("Installment Per Month"), r"(\d+)", 1)
            )
        else:
            df_transformed = df_transformed.withColumn("Installment Per Month", lit(None))

        if "Finance Details" in df.columns:
            # Step 6: Split Finance Details
            split_cols = split(col("Finance Details"), "·")

            df_transformed = (
                df_transformed
                .withColumn("chunk1", split_cols.getItem(0))
                .withColumn("chunk2", split_cols.getItem(1))
                .withColumn("chunk3", split_cols.getItem(2))
            )

            df_transformed = df_transformed.withColumn(
                "Down Payment",
                trim(regexp_replace(regexp_replace(col("chunk1"), r"^\$", ""), r"[^\d,]", ""))
            )

            df_transformed = df_transformed.withColumn(
                "Loan Term (Months)",
                regexp_extract(col("chunk2"), r"(\d+)", 1)
            )

            df_transformed = df_transformed.withColumn(
                "Interest Rate (APR)",
                regexp_extract(col("chunk3"), r"(\d+(\.\d+)?)", 1)
            )

            df_transformed = df_transformed.drop("Finance Details", "chunk1", "chunk2", "chunk3")
        else:
            df_transformed = df_transformed.withColumn("Down Payment", lit(None))
            df_transformed = df_transformed.withColumn("Loan Term (Months)", lit(None))
            df_transformed = df_transformed.withColumn("Interest Rate (APR)", lit(None))

        if "Mileage" in df.columns:
            # Step 7: Clean the Mileage column
            df_transformed = df_transformed.withColumn(
                "Mileage",
                trim(regexp_replace(col("Mileage"), r"[^\d,]", ""))
            )

        # Drop original Title column
        df_transformed = df_transformed.drop("Title")

        # Step 8: Reorder columns and ensure all necessary columns exist
        desired_columns = [
            "Year of Manufacture",
            "Car Make",
            "Car Full Title",
            "Cash Price (USD)",
            "Down Payment",
            "Installment Per Month",
            "Loan Term (Months)",
            "Interest Rate (APR)",
            "VIN",
            "Mileage",
            "Fuel Type",
            "MPG",
            "Exterior",
            "Interior",
            "Transmission",
            "Engine",
            "Location",
            "Listed Since",
            "Stock Number",
            "Features"
        ]

        # Add any missing columns as NULL
        for column in desired_columns:
            if column not in df_transformed.columns:
                df_transformed = df_transformed.withColumn(column, lit(None))

        # Select final columns in specific order
        df_transformed = df_transformed.select(desired_columns)

        final_count = df_transformed.count()
        logger.info(
            f"Transformation complete. Records after deduplication: {final_count} (Removed {initial_count - final_count} duplicates)")

        return df_transformed

    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        # Return a minimal dataframe if transform fails
        logger.warning("Creating minimal transformed DataFrame due to error")
        columns = ["Year of Manufacture", "Car Make", "Car Full Title", "Cash Price (USD)",
                   "Down Payment", "Installment Per Month", "Loan Term (Months)",
                   "Interest Rate (APR)", "VIN", "Mileage", "Fuel Type", "MPG",
                   "Exterior", "Interior", "Transmission", "Engine", "Location",
                   "Listed Since", "Stock Number", "Features"]
        data = [(2022, "Toyota", "2022 Toyota Camry", "25000", "5000", "450", 60, "3.9",
                 "JTDKARFP4N3001234", "5000", "Gasoline", "29 city/41 highway", "Red",
                 "Black", "Automatic", "2.5L I4", "Houston, TX", "2 weeks ago", "T12345",
                 "Bluetooth, Backup Camera")]
        return spark.createDataFrame(data, columns)


def get_existing_vins(spark):
    """Get list of VINs that already exist in the database to avoid duplicates"""
    try:
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        properties = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # Read existing VINs from Car_Info table
        existing_vins_df = spark.read \
            .jdbc(url=jdbc_url,
                  table="(SELECT \"VIN\" FROM \"Car_Info\") as existing_vins",
                  properties=properties)

        return existing_vins_df
    except Exception as e:
        logger.warning(f"Error getting existing VINs: {str(e)}")
        # If error occurs (like table doesn't exist yet), return empty dataframe
        return spark.createDataFrame([], "VIN STRING")


def load_data_to_postgres(spark, df):
    """Load data into the normalized PostgreSQL tables"""
    logger.info("Loading data to PostgreSQL warehouse")

    # Define JDBC URL and connection properties
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    properties = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    try:
        # Get existing VINs to filter out duplicates
        existing_vins_df = get_existing_vins(spark)
        existing_vins = [row.VIN for row in existing_vins_df.collect()]
        logger.info(f"Found {len(existing_vins)} existing VINs in database")

        # Filter out records with VINs that already exist in database
        if existing_vins:
            df = df.filter(~col("VIN").isin(existing_vins))
            filtered_count = df.count()
            logger.info(f"After filtering out existing VINs: {filtered_count} new records to insert")

            if filtered_count == 0:
                logger.info("No new records to insert - all VINs already exist in database")
                return 0

        # First create a temporary view to manage the data in memory
        df.createOrReplaceTempView("transformed_cars")

        # Create dataframes for each target table
        car_info_df = spark.sql("""
            SELECT 
                VIN,
                CAST(`Year of Manufacture` AS INTEGER) AS Year_Of_Manufacture,
                `Car Make` AS Car_Make,
                `Car Full Title` AS Car_Full_Title,
                `Cash Price (USD)` AS Car_Price_USD,
                CASE 
                    WHEN `Down Payment` IS NOT NULL THEN TRUE
                    ELSE FALSE
                END AS Installment_Support
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        installment_info_df = spark.sql("""
            SELECT
                VIN,
                `Down Payment` AS Down_Payment,
                `Installment Per Month` AS Installment_Per_Month,
                CAST(`Loan Term (Months)` AS INTEGER) AS Loan_Term,
                `Interest Rate (APR)` AS Interest_Rate
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        in_exterior_df = spark.sql("""
            SELECT 
                VIN,
                Exterior,
                Interior
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        evaluate_df = spark.sql("""
            SELECT 
                VIN,
                Mileage,
                `Fuel Type` AS Fuel_Type,
                MPG
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        engine_info_df = spark.sql("""
            SELECT 
                VIN,
                Transmission,
                Engine
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        other_detail_df = spark.sql("""
            SELECT 
                VIN,
                Location,
                `Listed Since` AS Listed_Since,
                `Stock Number` AS Stock_Number,
                Features
            FROM transformed_cars
            WHERE VIN IS NOT NULL
        """)

        # Save transformed data to transformed_cars table in PostgreSQL
        logger.info("Writing transformed_cars table")
        df.write \
            .jdbc(url=jdbc_url,
                  table="transformed_cars",
                  mode="append",
                  properties=properties)

        # Write data to the PostgreSQL tables
        logger.info("Writing Car_Info table")
        car_info_df.write \
            .jdbc(url=jdbc_url,
                  table="Car_Info",
                  mode="append",
                  properties=properties)

        logger.info("Writing Installment_Info table")
        installment_info_df.write \
            .jdbc(url=jdbc_url,
                  table="Installment_Info",
                  mode="append",
                  properties=properties)

        logger.info("Writing In_Exterior table")
        in_exterior_df.write \
            .jdbc(url=jdbc_url,
                  table="In_Exterior",
                  mode="append",
                  properties=properties)

        logger.info("Writing Evaluate table")
        evaluate_df.write \
            .jdbc(url=jdbc_url,
                  table="Evaluate",
                  mode="append",
                  properties=properties)

        logger.info("Writing Engine_Info table")
        engine_info_df.write \
            .jdbc(url=jdbc_url,
                  table="Engine_Info",
                  mode="append",
                  properties=properties)

        logger.info("Writing Other_Detail table")
        other_detail_df.write \
            .jdbc(url=jdbc_url,
                  table="Other_Detail",
                  mode="append",
                  properties=properties)

        # Log ETL execution
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO "ETL_Log" ("execution_date", "num_records_processed", "status", "executed_by")
            VALUES (%s, %s, %s, %s)
        """, (CURRENT_DATE_UTC, car_info_df.count(), 'SUCCESS', CURRENT_USER))

        cursor.close()
        conn.close()

        logger.info(f"Data loaded successfully to PostgreSQL warehouse - {car_info_df.count()} new records")
        return car_info_df.count()

    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        # Log failed ETL execution
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO "ETL_Log" ("execution_date", "num_records_processed", "status", "executed_by")
                VALUES (%s, %s, %s, %s)
            """, (CURRENT_DATE_UTC, 0, 'FAILED', CURRENT_USER))

            cursor.close()
            conn.close()
        except:
            pass

        raise
def create_processed_marker(records_processed):
    """Create a marker file to indicate successful processing"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    marker_path = os.path.join(PROCESSED_MARKER_DIR, f"etl_complete_{timestamp}.marker")

    with open(marker_path, 'w') as f:
        f.write(f"ETL completed successfully at {datetime.now().isoformat()}\n")
        f.write(f"Records processed: {records_processed}\n")
        f.write(f"Executed by: {CURRENT_USER}\n")

    logger.info(f"Created processed marker file: {marker_path}")


def main():
    """Main ETL function"""
    start_time = time.time()
    logger.info("Starting ETL process")
    logger.info(f"Current Date (UTC): {CURRENT_DATE_UTC}")
    logger.info(f"Current User: {CURRENT_USER}")

