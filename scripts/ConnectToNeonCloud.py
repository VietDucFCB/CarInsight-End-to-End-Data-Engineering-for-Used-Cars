#!/usr/bin/env python
import os
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Configure logging
logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "neon_migration.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def migrate_database():
    """
    Migrate data from local PostgreSQL to Neon cloud database
    """
    # Get database credentials from environment variables or use defaults
    local_db = {
        "dbname": os.getenv("LOCAL_DB_NAME", "car_warehouse"),  # Updated to match ETL.py
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", "postgres"),  # Use env var for security
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": os.getenv("LOCAL_DB_PORT", "5432")
    }

    neon_db = {
        "dbname": os.getenv("NEON_DB_NAME", "neondb"),
        "user": os.getenv("NEON_DB_USER", "neondb_owner"),
        "password": os.getenv("NEON_DB_PASSWORD", "npg_sYbjTQg5GUn0"),  # Use env var for security
        "host": os.getenv("NEON_DB_HOST", "ep-plain-base-a5et2w5s-pooler.us-east-2.aws.neon.tech"),
        "port": os.getenv("NEON_DB_PORT", "5432"),
        "sslmode": "require"
    }

    try:
        # Kết nối đến database local
        logger.info("Connecting to local database...")
        local_conn = psycopg2.connect(
            dbname=local_db["dbname"],
            user=local_db["user"],
            password=local_db["password"],
            host=local_db["host"],
            port=local_db["port"]
        )
        local_cursor = local_conn.cursor()

        # Kết nối đến database Neon
        logger.info("Connecting to Neon cloud database...")
        neon_conn = psycopg2.connect(
            dbname=neon_db["dbname"],
            user=neon_db["user"],
            password=neon_db["password"],
            host=neon_db["host"],
            port=neon_db["port"],
            sslmode=neon_db["sslmode"]
        )
        neon_cursor = neon_conn.cursor()

        # Lấy danh sách tất cả các bảng từ database local
        local_cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [table[0] for table in local_cursor.fetchall()]

        logger.info(f"Found {len(tables)} tables to migrate: {tables}")

        # Migrate từng bảng
        for table in tables:
            logger.info(f"Migrating table {table}...")

            # Lấy schema của bảng
            local_cursor.execute(f"""
                SELECT column_name, data_type, character_maximum_length,
                       is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table}' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            columns = local_cursor.fetchall()

            # Tạo bảng trong Neon
            try:
                # Check if table exists in Neon
                neon_cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name = '{table}'
                    );
                """)
                table_exists = neon_cursor.fetchone()[0]

                if table_exists:
                    logger.info(f"  Table {table} already exists in Neon, truncating...")
                    neon_cursor.execute(f'TRUNCATE TABLE "{table}" CASCADE;')
                    neon_conn.commit()
                else:
                    # Create the table in Neon
                    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
                    for i, (col_name, data_type, max_length, is_nullable, default) in enumerate(columns):
                        create_table_sql += f'    "{col_name}" {data_type}'
                        if max_length:
                            create_table_sql += f'({max_length})'

                        if default:
                            create_table_sql += f' DEFAULT {default}'

                        if is_nullable == 'NO':
                            create_table_sql += ' NOT NULL'

                        if i < len(columns) - 1:
                            create_table_sql += ',\n'
                        else:
                            create_table_sql += '\n'

                    create_table_sql += ');'

                    # Execute table creation
                    neon_cursor.execute(create_table_sql)
                    neon_conn.commit()
                    logger.info(f"  Created table {table}")

                # Get data from local database
                local_cursor.execute(f'SELECT * FROM "{table}"')
                rows = local_cursor.fetchall()

                if rows:
                    # Get column names
                    col_names = [desc[0] for desc in local_cursor.description]

                    # Import data into Neon
                    placeholders = ', '.join(['%s'] * len(col_names))
                    column_list = ', '.join([f'"{col}"' for col in col_names])

                    # Create insert statement
                    insert_sql = f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})'

                    # Insert batch for better performance
                    batch_size = 1000
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i + batch_size]
                        neon_cursor.executemany(insert_sql, batch)
                        neon_conn.commit()
                        logger.info(f"  Migrated {min(i + batch_size, len(rows))}/{len(rows)} records to table {table}")

                    logger.info(f"  Successfully migrated {len(rows)} records to table {table}")
                else:
                    logger.info(f"  Table {table} is empty, no data to migrate")

            except Exception as e:
                logger.error(f"  Error migrating table {table}: {e}")
                neon_conn.rollback()  # Rollback on error

        logger.info("Migration completed successfully!")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        # Close all connections
        if 'local_cursor' in locals() and local_cursor:
            local_cursor.close()
        if 'local_conn' in locals() and local_conn:
            local_conn.close()
        if 'neon_cursor' in locals() and neon_cursor:
            neon_cursor.close()
        if 'neon_conn' in locals() and neon_conn:
            neon_conn.close()

        logger.info("Database connections closed")


if __name__ == "__main__":
    migrate_database()