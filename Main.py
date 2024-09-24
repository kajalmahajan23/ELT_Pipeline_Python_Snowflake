import requests 
import pandas as pd
import snowflake.connector

# Prompt user to input Snowflake connection details
def get_snowflake_credentials():
    SNOWFLAKE_USER = input("Enter your Snowflake user: ")
    SNOWFLAKE_PASSWORD = input("Enter your Snowflake password: ")
    SNOWFLAKE_ACCOUNT = input("Enter your Snowflake account (e.g., abc12345.us-east-1): ")
    SNOWFLAKE_DATABASE = input("Enter your Snowflake database: ")
    SNOWFLAKE_SCHEMA = input("Enter your Snowflake schema: ")
    SNOWFLAKE_WAREHOUSE = input("Enter your Snowflake warehouse: ")

    return {
        'user': SNOWFLAKE_USER,
        'password': SNOWFLAKE_PASSWORD,
        'account': SNOWFLAKE_ACCOUNT,
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA,
        'warehouse': SNOWFLAKE_WAREHOUSE
    }

# Extract the data from NYC Open Data
def extract_data(limit=5000):
    url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    params = {"$limit": limit}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        print(f"Extracted {len(df)} rows of data.")
        return df
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# Connect to Snowflake
def connect_to_snowflake(credentials):
    conn = snowflake.connector.connect(
        user=credentials['user'],
        password=credentials['password'],
        account=credentials['account'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema']
    )
    return conn

def create_database_if_not_exists(conn, database_name):
    """Create the database if it doesn't exist."""
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
    conn.cursor().execute(create_database_query)
    print(f"Database '{database_name}' is ready.")

def create_schema_if_not_exists(conn, schema_name):
    """Create the schema if it doesn't exist."""
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    conn.cursor().execute(create_schema_query)
    print(f"Schema '{schema_name}' is ready.")

# Create the table in Snowflake
def create_table(conn):
    create_table_query = """
    CREATE OR REPLACE TABLE restaurant_inspections (
        camis STRING,
        dba STRING,
        boro STRING,
        inspection_date STRING,
        action STRING,
        violation_code STRING,
        violation_description STRING,
        critical_flag STRING,
        score STRING,
        grade STRING,
        grade_date STRING
    );
    """
    conn.cursor().execute(create_table_query)
    print("Table created successfully.")

# Load the data into Snowflake
def load_data_to_snowflake(conn, df):
    # Clean and prepare data for Snowflake
    df = df[['camis', 'dba', 'boro', 'inspection_date', 'action', 'violation_code', 
             'violation_description', 'critical_flag', 'score', 'grade', 'grade_date']].fillna("")

    # Convert dataframe to list of tuples for Snowflake insertion
    rows = [tuple(row) for row in df.to_numpy()]
    cursor = conn.cursor()
    
    # Insert data into Snowflake
    insert_query = f"INSERT INTO restaurant_inspections VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Loaded {len(rows)} records into Snowflake.")

# Transform the data in Snowflake
def transform_data(conn):
    query = """
    SELECT 
        boro, 
        YEAR(TO_TIMESTAMP(inspection_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF')) AS year, 
        COUNT(*) AS inspections_count
    FROM restaurant_inspections
    GROUP BY boro, year
    ORDER BY boro, year;
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert results to a DataFrame
    df_transformed = pd.DataFrame(results, columns=['boro', 'year', 'inspections_count'])
    return df_transformed

# Main function to run the ELT pipeline
def main():
    # Step 1: Get Snowflake credentials from the user
    credentials = get_snowflake_credentials()
    
    conn = None  # Initialize conn to None
    try:
        # Step 2: Connect to Snowflake
        conn = connect_to_snowflake(credentials)
        
        # Create database and schema
        create_database_if_not_exists(conn, credentials['database'])
        create_schema_if_not_exists(conn, credentials['schema'])
        
        # Step 3: Create the table in Snowflake
        create_table(conn)
        
        # Step 4: Extract the data
        df = extract_data()
        
        # Step 5: Load the data into Snowflake
        load_data_to_snowflake(conn, df)
        
        # Step 6: Transform the data and get the results
        df_transformed = transform_data(conn)
        
        # Print the transformed data
        print("Transformed Data:")
        print(df_transformed)
        
    finally:
        if conn:
            conn.close()  # Ensure the connection is closed 

if __name__ == "__main__":
    main()
