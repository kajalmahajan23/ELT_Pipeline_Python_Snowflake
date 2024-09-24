# ELT Pipeline with Snowflake

This project implements an ELT (Extract, Load, Transform) pipeline using Python and Snowflake. The pipeline fetches restaurant inspection data from NYC Open Data, loads it into a Snowflake database, and performs data transformation to aggregate inspection counts by borough and year.

## Project Structure

```
├── Main.py                     # Main script to run the pipeline
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

## Prerequisites

- Python 3.x
- Snowflake account
- NYC Open Data API access

## Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/kajalmahajan23/ELT_Pipeline_Python_Snowflake.git
    cd ELT_Pipeline_Python_Snowflake
    ```

2. Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Run the `Main.py` script:
    ```bash
    python Main.py
    ```

2. Follow the prompts to enter your Snowflake connection details.

3. The script will fetch data from NYC Open Data, load it into Snowflake, and perform transformations.

## Functions

- `get_snowflake_credentials()`: Prompts the user for Snowflake connection details.
- `extract_data(limit=5000)`: Extracts data from the NYC Open Data API.
- `connect_to_snowflake(credentials)`: Establishes a connection to Snowflake.
- `create_database_if_not_exists(conn)`: Creates the database in Snowflake if it doesn't exist.
- `create_schema_if_not_exists(conn)`: Creates the schema in Snowflake if it doesn't exist.
- `create_table(conn)`: Creates the table in Snowflake to store the data.
- `load_data_to_snowflake(conn, df)`: Loads the extracted data into Snowflake.
- `transform_data(conn)`: Transforms the data in Snowflake.

## License

This project is licensed under the MIT License.
