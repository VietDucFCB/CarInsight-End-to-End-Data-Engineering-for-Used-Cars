import psycopg2
import pandas as pd
def migrate_database():
    # Kết nối đến database local
    print("Kết nối đến database local...")
    local_conn = psycopg2.connect(
        dbname="DataWarehouse",
        user="postgres",
        password="Kkagiuma2004@",
        host="localhost",
        port="5432"
    )
    local_cursor = local_conn.cursor()

    # Kết nối đến database Neon
    print("Kết nối đến Neon...")
    neon_conn = psycopg2.connect(
        dbname="neondb",
        user="neondb_owner",
        password="npg_sYbjTQg5GUn0",
        host="ep-plain-base-a5et2w5s-pooler.us-east-2.aws.neon.tech",
        port="5432",
        sslmode="require"
    )
    neon_cursor = neon_conn.cursor()

    # Lấy danh sách tất cả các bảng từ database local
    local_cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [table[0] for table in local_cursor.fetchall()]

    print(f"Tìm thấy {len(tables)} bảng để migrate: {tables}")

    # Migrate từng bảng
    for table in tables:
        print(f"Đang migrate bảng {table}...")

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

            # Thực hiện tạo bảng
            neon_cursor.execute(create_table_sql)
            neon_conn.commit()
            print(f"  Đã tạo bảng {table}")

            # Lấy dữ liệu từ local
            local_cursor.execute(f'SELECT * FROM "{table}"')
            rows = local_cursor.fetchall()

            if rows:
                # Lấy tên các cột
                col_names = [desc[0] for desc in local_cursor.description]

                # Import dữ liệu vào Neon
                placeholders = ', '.join(['%s'] * len(col_names))
                column_list = ', '.join([f'"{col}"' for col in col_names])

                # Tạo câu lệnh insert
                insert_sql = f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})'

                # Thực hiện insert từng hàng
                for row in rows:
                    neon_cursor.execute(insert_sql, row)

                neon_conn.commit()
                print(f"  Đã migrate {len(rows)} bản ghi vào bảng {table}")
        except Exception as e:
            print(f"  Lỗi khi migrate bảng {table}: {e}")

    local_cursor.close()
    local_conn.close()
    neon_cursor.close()
    neon_conn.close()
    print("Migration hoàn tất!")

if __name__ == "__main__":
    migrate_database()