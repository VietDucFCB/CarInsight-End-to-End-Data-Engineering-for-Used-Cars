from sqlalchemy import create_engine, text

# Database connection strings
source_db_url = "postgresql://postgres:Kkagiuma2004%40@localhost:5432/DataWarehouse"
target_db_url = "postgresql://postgres:Kkagiuma2004%40@localhost:5432/Analytical_DB"

# Create database engines
source_engine = create_engine(source_db_url)
target_engine = create_engine(target_db_url)

# Table creation queries for Analytical_DB (giữ nguyên kiểu dữ liệu mong muốn)
create_tables = {
    "Car_Info": """
        CREATE TABLE IF NOT EXISTS Car_Info (
            VIN VARCHAR(17) PRIMARY KEY,
            Year_Of_Manufacture INT,
            Car_Make VARCHAR(50),
            Car_Full_Title VARCHAR(100),
            Car_Price_USD DECIMAL(15, 2),
            Installment_Support BOOLEAN
        );
    """,
    "Installment_Info": """
        CREATE TABLE IF NOT EXISTS Installment_Info (
            VIN VARCHAR(17),
            Down_Payment DECIMAL(15, 2),
            Installment_Per_Month DECIMAL(15, 2),
            Loan_Term INT,
            Interest_Rate DECIMAL(5, 2),
            FOREIGN KEY (VIN) REFERENCES Car_Info(VIN)
        );
    """,
    "In_Exterior": """
        CREATE TABLE IF NOT EXISTS In_Exterior (
            VIN VARCHAR(17),
            Exterior VARCHAR(50),
            Interior VARCHAR(50),
            FOREIGN KEY (VIN) REFERENCES Car_Info(VIN)
        );
    """,
    "Evaluate": """
        CREATE TABLE IF NOT EXISTS Evaluate (
            VIN VARCHAR(17),
            Mileage INT,
            Fuel_Type VARCHAR(20),
            MPG DECIMAL(5, 2),
            FOREIGN KEY (VIN) REFERENCES Car_Info(VIN)
        );
    """,
    "Engine_Info": """
        CREATE TABLE IF NOT EXISTS Engine_Info (
            VIN VARCHAR(17),
            Transmission VARCHAR(50),
            Engine VARCHAR(50),
            FOREIGN KEY (VIN) REFERENCES Car_Info(VIN)
        );
    """,
    "Other_Detail": """
        CREATE TABLE IF NOT EXISTS Other_Detail (
            VIN VARCHAR(17),
            Location VARCHAR(100),
            Listed_Since DATE,
            Stock_Number VARCHAR(20),
            Features TEXT,
            FOREIGN KEY (VIN) REFERENCES Car_Info(VIN)
        );
    """
}

# Data insertion queries with explicit casting from TEXT
insert_queries = {
    "Car_Info": """
        INSERT INTO Car_Info (VIN, Year_Of_Manufacture, Car_Make, Car_Full_Title, Car_Price_USD, Installment_Support)
        SELECT 
            VIN::VARCHAR(17),
            CAST("Year of Manufacture" AS INT),
            "Car Make"::VARCHAR(50),
            "Car Full Title"::VARCHAR(100),
            CAST("Cash Price (USD)" AS DECIMAL(15, 2)),
            CASE 
                WHEN "Down Payment" IS NULL THEN TRUE
                ELSE FALSE
            END AS Installment_Support
        FROM cars_inventory;
    """,
    "Installment_Info": """
        INSERT INTO Installment_Info (VIN, Down_Payment, Installment_Per_Month, Loan_Term, Interest_Rate)
        SELECT 
            VIN::VARCHAR(17),
            CAST("Down Payment" AS DECIMAL(15, 2)),
            CAST("Installment Per Month" AS DECIMAL(15, 2)),
            CAST("Loan Term (Months)" AS INT),
            CAST("Interest Rate (APR)" AS DECIMAL(5, 2))
        FROM cars_inventory;
    """,
    "In_Exterior": """
        INSERT INTO In_Exterior (VIN, Exterior, Interior)
        SELECT 
            VIN::VARCHAR(17),
            Exterior::VARCHAR(50),
            Interior::VARCHAR(50)
        FROM cars_inventory;
    """,
    "Evaluate": """
        INSERT INTO Evaluate (VIN, Mileage, Fuel_Type, MPG)
        SELECT 
            VIN::VARCHAR(17),
            CAST(Mileage AS INT),
            "Fuel Type"::VARCHAR(20),
            CAST(MPG AS DECIMAL(5, 2))
        FROM cars_inventory;
    """,
    "Engine_Info": """
            INSERT INTO Engine_Info (VIN, Transmission, Engine)
            SELECT 
                VIN::VARCHAR(17),
                Transmission::VARCHAR(50),
                Engine::VARCHAR(50)
            FROM cars_inventory;
    """,
    "Other_Detail": """
        INSERT INTO Other_Detail (VIN, Location, Listed_Since, Stock_Number, Features)
        SELECT 
            VIN::VARCHAR(17),
            Location::VARCHAR(100),
            CAST("Listed Since" AS DATE),
            "Stock Number"::VARCHAR(20),
            Features::TEXT
        FROM cars_inventory;
    """
}

try:
    with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
        # Create tables if they don’t exist
        for table, create_query in create_tables.items():
            target_conn.execute(text(create_query))
            print(f"Table {table} created or already exists.")

        # Insert data into Car_Info first (due to foreign key constraints)
        target_conn.execute(text(insert_queries["Car_Info"]))
        print("Data loaded into Car_Info successfully.")

        # Insert data into remaining tables
        for table, query in {k: v for k, v in insert_queries.items() if k != "Car_Info"}.items():
            target_conn.execute(text(query))
            print(f"Data loaded into {table} successfully.")

        # Commit the transaction
        target_conn.commit()

    print("ETL process completed!")
except Exception as e:
    print(f"ETL process failed: {e}")