import psycopg2


def connect_to_db():
    try:
        # Connection parameters
        conn = psycopg2.connect(
            dbname="neondb",
            user="neondb_owner",
            password="npg_sYbjTQg5GUn0",
            host="ep-plain-base-a5et2w5s-pooler.us-east-2.aws.neon.tech",
            port="5432",
            sslmode="require"
        )

        print("Connection established successfully!")

        # Test the connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"PostgreSQL database version: {db_version[0]}")

        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


# Use the connection
conn = connect_to_db()
if conn:
    # Do something with the connection
    # conn.close()  # Uncomment to close connection when done
    pass