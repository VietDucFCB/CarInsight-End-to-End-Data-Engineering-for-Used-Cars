import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
import re
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Used Car Recommendation System",
    page_icon="🚗",
    layout="wide"
)

@st.cache_resource
def get_connection():
    """Create a connection to Neon PostgreSQL database"""
    try:
        # Option 1: Use individual parameters
        db_params = st.secrets["database"]
        conn = psycopg2.connect(
            dbname=db_params["dbname"],
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
            sslmode=db_params["sslmode"]
        )

        # Option 2: Or use the connection string directly
        # conn = psycopg2.connect(st.secrets["database"]["url"])

        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# Function to clean price strings and convert to float
def clean_price(price_str):
    if not price_str:
        return 0
    # Remove currency symbols and commas
    return float(re.sub(r'[^\d.]', '', str(price_str)))


# Function to fetch car recommendations based on user inputs
def fetch_car_recommendations(year=None, car_make=None, price_min=None, price_max=None,
                              installment_support=None, engine=None, fuel_type=None, features=None):
    conn = get_connection()
    if not conn:
        return pd.DataFrame()

    try:
        cursor = conn.cursor()

        # Base query to join all tables - FIXED with proper double quotes for case-sensitive identifiers
        base_query = """
        SELECT 
            ci."VIN", ci."Year_Of_Manufacture", ci."Car_Make", ci."Car_Full_Title", ci."Car_Price_USD", ci."Installment_Support",
            ie."Exterior", ie."Interior",
            ev."Mileage", ev."Fuel_Type", ev."MPG",
            ei."Transmission", ei."Engine",
            od."Location", od."Listed_Since", od."Stock_Number", od."Features",
            ii."Down_Payment", ii."Installment_Per_Month", ii."Loan_Term", ii."Interest_Rate"
        FROM public."Car_Info" ci
        LEFT JOIN public."In_Exterior" ie ON ci."VIN" = ie."VIN"
        LEFT JOIN public."Evaluate" ev ON ci."VIN" = ev."VIN"
        LEFT JOIN public."Engine_Info" ei ON ci."VIN" = ei."VIN"
        LEFT JOIN public."Other_Detail" od ON ci."VIN" = od."VIN"
        LEFT JOIN public."Installment_Info" ii ON ci."VIN" = ii."VIN"
        WHERE 1=1
        """

        # Dynamic conditions based on user input - FIXED with proper quoting
        conditions = []
        params = []

        if year:
            conditions.append('ci."Year_Of_Manufacture" = %s')
            params.append(year)

        if car_make:
            conditions.append('ci."Car_Make" ILIKE %s')
            params.append(f"%{car_make}%")

        # Handle price range
        if price_min is not None:
            conditions.append(
                'CAST(REPLACE(REPLACE(REPLACE(ci."Car_Price_USD", \'$\', \'\'), \',\', \'\'), \' \', \'\') AS NUMERIC) >= %s')
            params.append(price_min)

        if price_max is not None:
            conditions.append(
                'CAST(REPLACE(REPLACE(REPLACE(ci."Car_Price_USD", \'$\', \'\'), \',\', \'\'), \' \', \'\') AS NUMERIC) <= %s')
            params.append(price_max)

        if installment_support is not None:
            conditions.append('ci."Installment_Support" = %s')
            params.append(installment_support)

        if engine:
            conditions.append('ei."Engine" ILIKE %s')
            params.append(f"%{engine}%")

        if fuel_type:
            conditions.append('ev."Fuel_Type" ILIKE %s')
            params.append(f"%{fuel_type}%")

        if features:
            conditions.append('od."Features" ILIKE %s')
            params.append(f"%{features}%")

        # Combine conditions into the query
        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        # Add sorting by price (ascending) - FIXED with proper quoting
        base_query += ' ORDER BY CAST(REPLACE(REPLACE(REPLACE(ci."Car_Price_USD", \'$\', \'\'), \',\', \'\'), \' \', \'\') AS NUMERIC) ASC'

        # Execute the query
        cursor.execute(base_query, params)
        results = cursor.fetchall()

        # Get column names for the DataFrame
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        cursor.close()
        return df

    except Exception as e:
        st.error(f"Error executing query: {e}")
        if 'cursor' in locals():
            cursor.close()
        return pd.DataFrame()


# Format price display
def format_price(price):
    if not price or pd.isna(price):
        return "N/A"
    try:
        cleaned_price = clean_price(price)
        return f"${cleaned_price:,.2f}"
    except:
        return str(price)


# Fix for the Other_Detail table name
def fix_table_name(table_name):
    # Check if the table name is actually "Other_Details" in the database
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name IN ('Other_Detail', 'Other_Details')
                AND table_schema = 'public'
            """)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0]
        except Exception as e:
            st.error(f"Error checking table name: {e}")
    return table_name


# UI Components
def main():
    st.title("🚗 Used Car Recommendation System")

    # Check if Other_Detail table exists, otherwise use Other_Details
    other_detail_table = fix_table_name("Other_Detail")

    st.write("""
    Find your perfect used car by entering your preferences below.
    Leave any fields blank if they don't matter to you.
    """)

    # Create sidebar for filters
    st.sidebar.header("Car Search Filters")

    # Year filter with current year as max
    current_year = datetime.now().year
    year = st.sidebar.number_input("Year of Manufacture",
                                   min_value=1900,
                                   max_value=current_year,
                                   value=None,
                                   step=1)

    # Car make with common brands as suggestions
    car_makes = ["", "Toyota", "Honda", "Ford", "Chevrolet", "BMW", "Mercedes-Benz",
                 "Audi", "Lexus", "Nissan", "Mazda", "Subaru", "Volkswagen", "Acura"]
    car_make = st.sidebar.selectbox("Car Make", options=car_makes, index=0)

    # Price range with slider
    col1, col2 = st.sidebar.columns(2)
    price_min = col1.number_input("Min Price ($)", min_value=0, value=0, step=1000)
    price_max = col2.number_input("Max Price ($)", min_value=0, value=100000, step=1000)

    # Other filters
    installment_support = st.sidebar.selectbox(
        "Installment Support",
        options=[None, True, False],
        format_func=lambda x: "Any" if x is None else ("Yes" if x else "No"),
        index=0
    )

    # Engine and fuel type
    engine_types = ["", "V6", "V8", "Inline-4", "Hybrid", "Electric"]
    engine = st.sidebar.selectbox("Engine Type", options=engine_types, index=0)

    fuel_types = ["", "Gas", "Diesel", "Electric", "Hybrid", "Natural Gas"]
    fuel_type = st.sidebar.selectbox("Fuel Type", options=fuel_types, index=0)

    # Features with multiselect
    common_features = ["Child Safety", "Sunroof", "Navigation", "Leather Seats", "Backup Camera",
                       "Bluetooth", "Heated Seats", "Third Row Seats", "AWD/4WD"]
    selected_features = st.sidebar.multiselect("Features", options=common_features)
    features = ", ".join(selected_features) if selected_features else None

    # Debug section
    if st.sidebar.checkbox("Show Query Debug Info", False):
        with st.sidebar.expander("Database Connection Info"):
            st.code("""
            dbname="DataWarehouse",
            user="postgres",
            host="localhost",
            port="5432"
            """)

            conn = get_connection()
            if conn:
                st.success("Successfully connected to database!")
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                    tables = cursor.fetchall()
                    st.write("Available tables:", [t[0] for t in tables])
                    cursor.close()
                except Exception as e:
                    st.error(f"Error listing tables: {e}")

    # Search button
    search_button = st.sidebar.button("Search for Cars", type="primary")

    if search_button:
        with st.spinner("Searching for cars matching your criteria..."):
            df = fetch_car_recommendations(year, car_make, price_min, price_max,
                                           installment_support, engine, fuel_type, features)

        if df.empty:
            st.warning("No cars found matching your criteria. Try adjusting your filters.")
        else:
            st.success(f"Found {len(df)} car(s) matching your criteria!")

            # Display results in a more organized way
            for i, row in df.iterrows():
                with st.expander(
                        f"{row['Car_Make']} {row['Car_Full_Title']} ({row['Year_Of_Manufacture']}) - {format_price(row['Car_Price_USD'])}"):

                    # Two columns for car info
                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("Basic Information")
                        st.write(f"**VIN:** {row['VIN']}")
                        st.write(f"**Year:** {row['Year_Of_Manufacture']}")
                        st.write(f"**Make:** {row['Car_Make']}")
                        st.write(f"**Model:** {row['Car_Full_Title']}")
                        st.write(f"**Price:** {format_price(row['Car_Price_USD'])}")
                        st.write(f"**Location:** {row['Location'] if not pd.isna(row['Location']) else 'N/A'}")
                        st.write(
                            f"**Listed Since:** {row['Listed_Since'] if not pd.isna(row['Listed_Since']) else 'N/A'}")

                        st.subheader("Engine & Performance")
                        st.write(f"**Engine:** {row['Engine'] if not pd.isna(row['Engine']) else 'N/A'}")
                        st.write(
                            f"**Transmission:** {row['Transmission'] if not pd.isna(row['Transmission']) else 'N/A'}")
                        st.write(f"**Fuel Type:** {row['Fuel_Type'] if not pd.isna(row['Fuel_Type']) else 'N/A'}")
                        st.write(f"**MPG:** {row['MPG'] if not pd.isna(row['MPG']) else 'N/A'}")
                        st.write(f"**Mileage:** {row['Mileage'] if not pd.isna(row['Mileage']) else 'N/A'}")

                    with col2:
                        st.subheader("Appearance")
                        st.write(f"**Exterior Color:** {row['Exterior'] if not pd.isna(row['Exterior']) else 'N/A'}")
                        st.write(f"**Interior Color:** {row['Interior'] if not pd.isna(row['Interior']) else 'N/A'}")

                        if row['Installment_Support'] or (not pd.isna(row['Down_Payment']) and row['Down_Payment']):
                            st.subheader("Financing Available")
                            st.write(
                                f"**Down Payment:** {format_price(row['Down_Payment']) if not pd.isna(row['Down_Payment']) else 'N/A'}")
                            st.write(
                                f"**Monthly Payment:** {format_price(row['Installment_Per_Month']) if not pd.isna(row['Installment_Per_Month']) else 'N/A'}")
                            st.write(
                                f"**Loan Term:** {row['Loan_Term'] if not pd.isna(row['Loan_Term']) else 'N/A'} months")
                            st.write(
                                f"**Interest Rate:** {row['Interest_Rate'] if not pd.isna(row['Interest_Rate']) else 'N/A'}%")

                        st.subheader("Features")
                        if not pd.isna(row['Features']) and row['Features']:
                            features_text = row['Features']
                            # Handle multiline features text
                            if isinstance(features_text, str):
                                features_list = [f.strip() for f in features_text.split('\n') if f.strip()]
                                for feature in features_list:
                                    st.write(f"• {feature}")
                            else:
                                st.write("• " + str(features_text))
                        else:
                            st.write("No features listed")

                    st.write(f"**Stock #:** {row['Stock_Number'] if not pd.isna(row['Stock_Number']) else 'N/A'}")

            # Also provide a raw data view
            with st.expander("View Raw Data"):
                st.dataframe(df)

                # Add a download button
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download Results as CSV",
                    csv,
                    "car_recommendations.csv",
                    "text/csv",
                    key='download-csv'
                )


if __name__ == "__main__":
    main()