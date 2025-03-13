from sqlalchemy import create_engine, text

# Database connection strings (use your actual password)
source_db_url = "postgresql://postgres:Kkagiuma2004@@localhost:5432/DataWarehouse"
target_db_url = "postgresql://postgres:Kkagiuma2004@@localhost:5432/Analytical_DB"

# Create database engines
source_engine = create_engine(source_db_url)
target_engine = create_engine(target_db_url)

# Define the queries
queries = {
    "Car_Info": """
        INSERT INTO Car_Info (VIN, Year_Of_Manufacture, Car_Make, Car_Full_Title, Car_Price_USD, Installment_Support)
        SELECT 
            VIN,
            "Year of Manufacture" AS Year_Of_Manufacture,
            "Car Make" AS Car_Make,
            "Car Full Title" AS Car_Full_Title,
            "Cash Price (USD)" AS Car_Price_USD,
            CASE 
                WHEN "Down Payment" IS NULL THEN 1
                ELSE 0
            END AS Installment_Support
        FROM cars_inventory;
    """,
    "Installment_Info": """
        INSERT INTO Installment_Info (VIN, Down_Payment, Installment_Per_Month, Loan_Term, Interest_Rate)
        SELECT 
            VIN,
            "Down Payment" AS Down_Payment,
            "Installment Per Month" AS Installment_Per_Month,
            "Loan Term (Months)" AS Loan_Term,
            "Interest Rate (APR)" AS Interest_Rate
        FROM cars_inventory;
    """,
    "In_Exterior": """
        INSERT INTO In_Exterior (VIN, Exterior, Interior)
        SELECT 
            VIN,
            Exterior,
            Interior
        FROM cars_inventory;
    """,
    "Evaluate": """
        INSERT INTO Evaluate (VIN, Mileage, Fuel_Type, MPG)
        SELECT 
            VIN,
            Mileage,
            "Fuel Type" AS Fuel_Type,
            MPG
        FROM cars_inventory;
    """,
    "Engine_Info": """
        INSERT INTO Engine_Info (VIN, Transmission, Engine)
        SELECT 
            VIN,
            Transmission,
            Engine
        FROM cars_inventory;
    """,
    "Other_Detail": """
        INSERT INTO Other_Detail (VIN, Location, Listed_Since, Stock_Number, Features)
        SELECT 
            VIN,
            Location,
            "Listed Since" AS Listed_Since,
            "Stock Number" AS Stock_Number,
            Features
        FROM cars_inventory;
    """
}

# Execute the queries
with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
    # Insert data into Car_Info first to maintain foreign key constraints
    target_conn.execute(text(queries["Car_Info"]))
    print(f"Data loaded into Car_Info successfully.")

    # Insert data into remaining tables
    for table, query in {k: v for k, v in queries.items() if k != "Car_Info"}.items():
        target_conn.execute(text(query))
        print(f"Data loaded into {table} successfully.")

print("ETL process completed!")